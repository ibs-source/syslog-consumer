/**
 * @class StreamProcessor
 * @classdesc
 * Manages a Redis stream consumer group: reconnects on failure,
 * drains backlog, polls for new entries, enqueues processing tasks,
 * and conditionally acknowledges/deletes processed messages.
 */

const { createClient } = require('redis');

class StreamProcessor {
    /**
     * Flag indicating a decrease in concurrency (halving).
     * @type {number}
     */
    static DECREASE = 0x0;

    /**
     * Flag indicating an increase in concurrency (doubling).
     * @type {number}
     */
    static INCREASE = 0x1;

    /**
     * Redis client instance used for all XGROUP, XREADGROUP, XPENDING, XACK and XDEL commands.
     * @type {import('redis').RedisClientType}
     */
    #redisClient;

    /**
     * Maximum number of messages processed in parallel.
     * @type {number}
     */
    #concurrency;

    /**
     * Counter of currently in-flight tasks for back-pressure control.
     * @type {number}
     */
    #inFlightCount = 0;

    /**
     * Flag indicating if the processor is in the process of shutting down.
     * @type {boolean}
     */
    #isTerminating = false;

    /**
     * Redis stream key (name) from which this processor will claim and read entries.
     * @type {string}
     */
    #stream;

    /**
     * Status function provided by Node-RED.
     * @type {(fill: string, shape: string, text: string) => void}
     */
    #status;

    /**
    * Callback invoked after serialization of each message.
    * Receives an object { payload: string, _redisId: string }.
    * @type {(msg: {payload: string; _redisId: string}) => void}
    */
    #onMessageCallback;

    /**
     * Logging function provided by Node-RED.
     * @type {(message: string, context: string) => void}
     */
    #logger;

    /**
     * Name of the Redis consumer group used for XREADGROUP operations.
     * @type {string}
     */
    #consumerGroup;

    /**
     * Identifier of this consumer instance within the consumer group.
     * @type {string}
     */
    #consumerName;

    /**
     * Adaptive reading state for dynamic COUNT and BLOCK adjustment.
     * @type {{count: number, block: number, fullReads: number}}
     */
    #adaptiveState = {
        count: 1e3,      // Start with 1000 messages
        block: 3e4,      // Start with 30s blocking
        fullReads: 0     // Counter for consecutive full reads
    };

    /**
     * Creates a StreamProcessor and opens a Redis connection.
     *
     * @param {string} url - Full Redis connection URI, e.g. 'redis://localhost:6379'.
     * @param {string} stream - Name of the Redis stream to consume.
     * @param {number} concurrency - Maximum number of messages processed in parallel.
     * @param {(fill: string, shape: string, text: string) => void} status - Status function provided by Node-RED.
     * @param {(msg: {payload:string; _redisId:string}) => void} onMessageCallback - Called when a message is dequeued and ready to send downstream.
     * @param {(message: string, context: string) => void} logger - Node-RED logger for lifecycle events and errors.
     */
    constructor(url, stream, concurrency, status, onMessageCallback, logger) {
        this.#stream = stream;
        this.#concurrency = concurrency;
        this.#onMessageCallback = onMessageCallback;
        this.#logger = logger;
        this.#status = status;
        const result = Math.random().toString(36).substring(2, 7);
        this.#consumerName = `name:${result}`;
        this.#consumerGroup = `grp:${stream}`;
        this.#redisClient = createClient({
            url
        });

        this.#redisClient.on('connect', () => this.#status('green', 'dot', 'connected'));
        this.#redisClient.on('reconnecting', () => this.#status('yellow', 'ring', 'reconnecting'));
        this.#redisClient.on('error', (err) => this.#logger(`Redis client error: ${err.message}`, 'stream-processor'));
        this.#redisClient.on('end', () => this.#status('red', 'ring', 'disconnected'));
    }

    /**
    * Recursively parses any nested JSON strings into native objects.
    * @param {*} value
    * @returns {*}
    */
    static parseRecursively(value) {
        switch (true) {
            case typeof value === 'string': {
                try {
                    const parsed = JSON.parse(value);
                    return StreamProcessor.parseRecursively(parsed);
                } catch {
                    return value;
                }
            }
            case Array.isArray(value):
                return value.map(item => StreamProcessor.parseRecursively(item));
            case value !== null && typeof value === 'object': {
                const result = {};
                for (const [key, item] of Object.entries(value))
                    result[key] = StreamProcessor.parseRecursively(item);
                return result;
            }
            default:
                return value;
        }
    }

    /**
     * Allows external entities (e.g. GlobalResource) to send logs
     * via the already injected logger (second node output).
     * @param {string} message
     * @param {string} [context]
     */
    log(message, context) {
        this.#logger(message, context);
    }

    /**
     * Attempts to connect to Redis and exits the process if it fails.
     * @private
     * @async
     * @returns {Promise<void>}
     */
    async #connectRedis() {
        try {
            await this.#redisClient.connect();
        } catch (err) {
            this.#logger(`Redis connection error: ${err.message}`, 'stream-processor');
        }
    }

    /**
     * Performs startup sequence:
     *   1. Creates or confirms the consumer group
     *   2. Reads and enqueues all pending backlog entries
     *   3. Claims and re-enqueues any in-flight messages older than idle threshold
     *   4. Starts the ongoing polling loop
     * @async
     * @returns {Promise<void>}
     */
    async initialize() {
        try {
            await this.#connectRedis();
            await this.#ensureConsumerGroup();
            await this.#recoverPendingAndClean();
            await this.#drainBacklog();
            this.#pollingMessages();
        } catch (err) {
            this.#logger(`Initialization error: ${err.message}`, 'stream-processor');
            throw err;
        }
    }

    /**
     * Removes group consumers that have no pending messages
     * and have been idle for more than 1 minutes.
     * @private
     * @async
     * @returns {Promise<void>}
     */
    async #cleanStaleConsumers() {
        if (!this.#redisClient.isOpen) {
            return;
        }
        const consumers = await this.#redisClient.xInfoConsumers(this.#stream, this.#consumerGroup);
        for (const who of consumers) {
            const { name, pending, idle } = who;
            if (name === this.#consumerName
                || pending !== 0
                || idle <= 6e4) continue;
            await this.#redisClient.xGroupDelConsumer(this.#stream, this.#consumerGroup, name);
            this.#logger(`Removed stale consumer "${name}" (idle ${idle}ms)`, 'stream-processor');
        }
    }

    /**
     * Creates the Redis consumer group 'grp:<stream>' at ID '0'.
     * Suppresses error if the group already exists (BUSYGROUP).
     * @private
     * @async
     * @returns {Promise<void>}
     */
    async #ensureConsumerGroup() {
        if (!this.#redisClient.isOpen) {
            return;
        }
        try {
            await this.#redisClient.xGroupCreate(this.#stream, this.#consumerGroup, '0', {
                MKSTREAM: true
            });
        } catch (err) {
            if (!err.message.includes('BUSYGROUP')) throw err;
        }
    }

    /**
     * Continuously reads from '0' using xReadGroup until no messages return, 
     * enqueuing them for processing and removal.
     * @private
     * @async
     * @returns {Promise<void>}
     */
    async #drainBacklog() {
        if (!this.#redisClient.isOpen) {
            return;
        }
        let cursor = '0';
        let errorCount = 0;
        while (true) {
            try {
                const streams = await this.#redisClient.xReadGroup(this.#consumerGroup, this.#consumerName, [{
                    key: this.#stream,
                    id: cursor
                }], {
                    COUNT: 2e4  // Use max count for draining backlog
                });
                errorCount = 0;
                const messages = (streams?.[0]?.messages) ?? [];
                if (messages.length === 0) break;
                if (messages[0].id === cursor) messages.shift();
                if (messages.length === 0) break;
                this.#logger(`Draining batch of ${messages.length} messages`, 'stream-processor');
                for (const msg of messages) {
                    this.#enqueueMessage(msg).catch(err => this.#logger(`Enqueue error: ${err.message}`, 'stream-processor'));
                }
                const last = messages.pop();
                if (typeof last !== 'object') break;
                cursor = last.id;
            } catch (err) {
                if (++errorCount >= 5) throw new Error(`Too many read errors on drain (stream=${this.#stream})`);
                const backoff = this.#backoffMs(errorCount);
                this.#logger(`Read error, backoff ${backoff}ms`, 'stream-processor');
                await this.#sleep(backoff);
            }
        }
    }

    /**
     * Retrieves pending entry summary, then pages through full range
     * to claim and re-enqueue entries idle longer than the threshold.
     * @private
     * @async
     * @returns {Promise<void>}
     */
    async #claimAllPending() {
        if (!this.#redisClient.isOpen) {
            return;
        }
        const batchSize = 1e3; // Process up to 1,000 entries at once
        const idleThreshold = 9e5; // 15 minutes
        let startId = '-';
        while (true) {
            try {
                const entries = await this.#redisClient.xPendingRange(this.#stream, this.#consumerGroup, startId, '+', batchSize);
                if (entries.length === 0) break;
                if (entries[0].id === startId) entries.shift();
                if (entries.length === 0) break;
                const idsToClaim = entries
                    .filter(entry => entry.millisecondsSinceLastDelivery >= idleThreshold)
                    .map(entry => entry.id);
                if (idsToClaim.length > 0) {
                    this.#logger(`Claiming ${idsToClaim.length} pending messages`, 'stream-processor');
                    const claimedMessages = await this.#redisClient.xClaim(this.#stream, this.#consumerGroup, this.#consumerName, 0, idsToClaim);
                    for (const msg of claimedMessages) {
                        this.#enqueueMessage(msg).catch(err => this.#logger(`Enqueue error on claimed message: ${err.message}`, 'stream-processor'));
                    }
                }
                const last = entries.pop();
                if (typeof last !== 'object') break;
                startId = last.id;
            } catch (err) {
                this.#logger(`Error during pending check: ${err.message}`, 'stream-processor');
                break;
            }
        }
    }

    /**
     * Runs an endless loop with adaptive reading:
     *   - Dynamically adjusts COUNT (1000-20000) and BLOCK (0-30s) based on load
     *   - Scales up COUNT when receiving full batches
     *   - Reduces BLOCK to 0 when at max COUNT and still receiving full batches
     *   - Scales back down when load decreases
     * Every 10 iterations reclaims pending and cleans stale consumers.
     * @private
     * @async
     * @returns {Promise<void>}
     */
    async #pollingMessages() {
        let iterations = 0;
        let errorCount = 0;
        while (!this.#isTerminating) {  // Check termination flag
            if (++iterations % 10 === 0) await this.#recoverPendingAndClean();
            try {
                if (!this.#redisClient.isOpen) {
                    break;
                }
                const resp = await this.#redisClient.xReadGroup(this.#consumerGroup, this.#consumerName, [{
                    key: this.#stream,
                    id: '>'
                }], {
                    COUNT: this.#adaptiveState.count,
                    BLOCK: this.#adaptiveState.block
                });
                const messages = resp?.[0]?.messages || [];
                // Adapt read parameters based on messages received
                this.#adaptReadParameters(messages.length);
                if (!messages.length) continue;
                for (const msg of messages) this.#enqueueMessage(msg);
                errorCount = 0;
            } catch (err) {
                if (this.#isTerminating) break;  // Exit the loop if we are terminating
                if (++errorCount >= 5) throw new Error(`Too many read errors on polling (uuid=${this.#stream})`);
                const backoff = this.#backoffMs(errorCount);
                this.#logger(`Polling error, backoff ${backoff}ms`, 'stream-processor');
                await this.#sleep(backoff);
            }
        }
        this.#logger(`Polling terminated for ${this.#consumerName}`, 'stream-processor');
    }

    /**
     * Adapts COUNT and BLOCK parameters based on the number of messages received.
     * - If we receive exactly COUNT messages, there's likely more load
     * - First scales COUNT up to 20000 (2e4)
     * - Then reduces BLOCK progressively to 0 if still receiving full batches
     * - Scales back down when receiving fewer messages
     * @param {number} messagesReceived - Number of messages in the last read
     * @private
     */
    #adaptReadParameters(messagesReceived) {
        const state = this.#adaptiveState;
        const receivedFullBatch = messagesReceived === state.count;
        const receivedLowBatch = messagesReceived < state.count / 2;
        switch (true) {
            // Full batch received - scale up
            case receivedFullBatch && state.count < 2e4:
                state.fullReads++;
                state.count = Math.min(state.count * 2, 2e4);
                break;
            // At max COUNT with full batches - reduce BLOCK
            case receivedFullBatch && state.count === 2e4 && state.fullReads >= 3 && state.block > 0:
                state.fullReads++;
                state.block = Math.max(0, state.block - 1e4); // Reduce by 10s
                break;
            // Still receiving full batches but not ready to change
            case receivedFullBatch:
                state.fullReads++;
                break;
            // Low batch with BLOCK not at max - restore BLOCK first
            case receivedLowBatch && state.block < 3e4:
                state.fullReads = 0;
                state.block = Math.min(3e4, state.block + 1e4);
                break;
            // Low batch with BLOCK at max - reduce COUNT
            case receivedLowBatch && state.count > 1e3:
                state.fullReads = 0;
                state.count = Math.max(1e3, state.count / 2);
                break;
            // Default - reset counter for partial reads
            default:
                state.fullReads = 0;
                break;
        }
    }

    /**
   * Periodically reclaims eligible pending messages and cleans stale consumers.
   * @private
   * @async
   * @returns {Promise<void>}
   */
    async #recoverPendingAndClean() {
        if (!this.#redisClient.isOpen) {
            return;
        }
        await this.#claimAllPending();
        await this.#cleanStaleConsumers();
    }

    /**
     * Pauses execution with adaptive intervals until the number
     * of in-flight tasks is below the concurrency threshold.
     * Delay scales with overload to reduce busy-waiting.
     * @private
     * @async
     * @returns {Promise<void>}
     */
    async #waitForCapacity() {
        const baseDelay = 50;
        while (this.#inFlightCount >= this.#concurrency) {
            const overload = this.#inFlightCount - this.#concurrency + 1;
            const delay = Math.min(1e3, baseDelay * overload);
            await this.#sleep(delay);
        }
    }

    /**
     * Enqueues a message for processing, tracking in-flight count.
     * @param {{ id: string; message: any }} msg - Object containing the Redis entry ID and the raw payload.
     * @private
     * @async
     * @returns {Promise<void>}
     */
    async #enqueueMessage(msg) {
        await this.#waitForCapacity();
        this.#inFlightCount++;
        this.#processMessageTask(msg)
            .catch(err => this.#logger(`Task error: ${err.message}`, 'stream-processor'))
            .finally(() => {
                this.#inFlightCount--;
            });
    }

    /**
     * Build message and invokes the callback.
     * @param {{ id: string; message: any }} msg - The message object.
     * @private
     * @async
     * @returns {Promise<void>}
     */
    async #processMessageTask(msg) {
        const raw = JSON.parse(JSON.stringify(msg.message));
        const message = StreamProcessor.parseRecursively(raw);
        this.#onMessageCallback({
            message: {
                payload: message
            },
            redis: {
                payload: {
                    id: msg.id
                }
            }
        });
    }

    /**
     * Acknowledges and deletes a message from the Redis stream and consumer group.
     * @param {string} id - ID of the message to acknowledge and delete.
     * @async
     * @returns {Promise<void>}
     */
    async acknowledgeAndDelete(id) {
        if (!this.#redisClient.isOpen) {
            return;
        }
        await this.#redisClient.xAck(this.#stream, this.#consumerGroup, id);
        await this.#redisClient.xDel(this.#stream, id);
    }

    /**
     * Dynamically adjusts the concurrency limit at runtime.
     * @param {number} newConcurrency - Desired concurrency (>=1).
     */
    setConcurrency(newConcurrency) {
        this.#concurrency = newConcurrency;
    }

    /**
     * @private
     * @method #backoffMs
     * @description Calculates an exponential backoff delay (1s * 2^(attempt-1)), up to 10s max.
     * @param {number} attempt - The current consecutive error attempt count.
     * @returns {number} Milliseconds of delay.
     */
    #backoffMs(attempt) {
        return Math.min(1e3 * (2 ** (attempt - 1)), 1e4);
    }

    /**
     * Pauses execution for the specified number of milliseconds.
     * @param {number} ms - Duration in milliseconds.
     * @private
     * @returns {Promise<void>}
     */
    #sleep(ms) {
        return new Promise(res => setTimeout(res, ms));
    }

    /**
     * Terminates the processor and closes the Redis connection.
     * @async
     * @returns {Promise<void>}
     */
    async shutdown() {
        this.#isTerminating = true;
        try {
            // Close Redis connection
            if (this.#redisClient.isOpen) {
                await this.#redisClient.quit();
                this.#logger('Redis connection closed successfully', 'stream-processor');
            }
        } catch (err) {
            this.#logger(`Error during Redis connection closure: ${err.message}`, 'stream-processor');
        }
    }
}

module.exports = StreamProcessor;
