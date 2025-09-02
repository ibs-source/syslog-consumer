/**
 * @class GlobalResource
 * @classdesc
 * Monitors process CPU and RAM usage and dynamically adjusts global concurrency
 * limits for registered stream processors using hysteresis thresholds.
 */

const os = require('os');
const pidusage = require('pidusage');

class GlobalResource {
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
     * Logging callback to record manager events and errors.
     * @type {(message: string, context: string) => void}
     */
    #logger;

    /**
     * Map of stream → processor instance, used to apply updated concurrency limits.
     * @type {Map<string, { setConcurrency: (number) => void }>}
     */
    #processors;

    /**
     * Sliding‐window buffer of recent CPU usage percentages.
     * @type {number[]}
     */
    #cpuSamples;

    /**
     * Sliding‐window buffer of recent RAM usage percentages.
     * @type {number[]}
     */
    #memSamples;

    /**
     * Handle returned by setInterval for the sampling loop; null when inactive.
     * @type {NodeJS.Timeout|null}
     */
    #intervalId;

    /**
     * Current global concurrency threshold in effect.
     * @type {number}
     */
    #currentConcurrency;

    /**
     * Configured upper bound for concurrency scaling.
     * @type {number}
     */
    #maxConcurrency;

    /**
     * Configured lower bound for concurrency scaling.
     * @type {number}
     */
    #minConcurrency;

    /**
     * Maximum number of samples kept in the sliding window.
     * @type {number}
     */
    #samplingCount;

    /**
     * Time interval (in ms) between usage samples.
     * @type {number}
     */
    #samplingIntervalMs;

    /**
     * CPU usage percentage above which concurrency will be decreased.
     * @type {number}
     */
    #upperCpuThresholdPercent;

    /**
     * CPU usage percentage below which concurrency will be increased.
     * @type {number}
     */
    #lowerCpuThresholdPercent;

    /**
     * RAM usage percentage above which concurrency will be decreased.
     * @type {number}
     */
    #upperMemThresholdPercent;

    /**
     * RAM usage percentage below which concurrency will be increased.
     * @type {number}
     */
    #lowerMemThresholdPercent;

    /**
     * Instantiates the resource manager.
     *
     * @param {Map<string, { setConcurrency: (number) => void }>} processors
     *   Map of stream to processor instances.
     * @param {Object} config
     * @param {number} config.maxConcurrency - Upper bound for concurrency scaling.
     * @param {number} config.minConcurrency - Lower bound for concurrency scaling.
     * @param {number} config.samplingCount - Number of samples used for averaging.
     * @param {number} config.samplingIntervalMs - Interval (ms) between samples.
     * @param {number} config.upperCpuThresholdPercent - CPU% to trigger a decrease.
     * @param {number} config.lowerCpuThresholdPercent - CPU% to trigger an increase.
     * @param {number} config.upperMemThresholdPercent - RAM% to trigger a decrease.
     * @param {number} config.lowerMemThresholdPercent - RAM% to trigger an increase.
     * @param {(message: string, context: string) => void} logger - Callback for logging manager events and errors.
     */
    constructor(processors, config, logger) {
        this.#logger = logger;
        this.#processors = processors;
        this.#cpuSamples = [];
        this.#memSamples = [];
        this.#intervalId = null;

        this.#maxConcurrency = GlobalResource.toInt(config.maxConcurrency, 10);
        this.#minConcurrency = GlobalResource.toInt(config.minConcurrency, 1);
        this.#currentConcurrency = this.#maxConcurrency;

        this.#samplingCount = GlobalResource.toInt(config.samplingCount, 10);
        this.#samplingIntervalMs = GlobalResource.toInt(config.samplingIntervalMs, 5000);
        this.#upperCpuThresholdPercent = GlobalResource.toInt(config.upperCpuThresholdPercent, 80);
        this.#lowerCpuThresholdPercent = GlobalResource.toInt(config.lowerCpuThresholdPercent, 30);
        this.#upperMemThresholdPercent = GlobalResource.toInt(config.upperMemThresholdPercent, 80);
        this.#lowerMemThresholdPercent = GlobalResource.toInt(config.lowerMemThresholdPercent, 30);
    }

    /**
     * Parses a value into an integer, returning a default if parsing fails.
     *
     * @param {string|number} value - The input value to convert.
     * @param {number} fallback - The fallback default if parsing fails.
     * @private
     * @returns {number} The parsed integer or fallback.
     */
    static toInt(value, fallback) {
        const parsed = parseInt(value, 10);
        return isNaN(parsed)
            ? fallback
            : parsed;
    }

    /**
     * Launches the periodic sampling loop at the configured interval.
     * @returns {void}
     */
    start() {
        if (this.#intervalId) return;
        this.#intervalId = setInterval(this.#handleSamplingCycle.bind(this), this.#samplingIntervalMs);
    }

    /**
     * Stops ongoing sampling by clearing the interval.
     * @returns {void}
     */
    stop() {
        if (!this.#intervalId) return;
        if (this.#processors.size > 0) return this.#logger(`Skip stop: still ${this.#processors.size} processor(s) active`, 'global-resource');
        clearInterval(this.#intervalId);
        this.#intervalId = null;
        this.#logger('Stopped resource sampling (no active processors)', 'global-resource');
    }

    /**
     * Retrieves the current global concurrency threshold.
     * @returns {number}
     */
    get concurrency() {
        return this.#currentConcurrency;
    }

    /**
     * Executes one monitoring cycle:
     *   1. Samples CPU and RAM usage
     *   2. Records samples and computes weighted averages
     *   3. Logs results and applies hysteresis-based scaling
     * @private
     * @async
     * @returns {void}
     */
    async #handleSamplingCycle() {
        try {
            const stats = await pidusage(process.pid);
            const memPercent = (stats.memory / os.totalmem()) * 100;
            this.#recordCpuSample(stats.cpu);
            this.#recordMemSample(memPercent);
            const avgCpu = this.#calculateWeightedAverage(this.#cpuSamples);
            const avgMem = this.#calculateWeightedAverage(this.#memSamples);
            this.#adjustConcurrencyIfNeeded(avgCpu, avgMem);
        } catch (err) {
            this.#logger(`Sampling error: ${err.message}`, 'global-resource');
        }
    }

    /**
     * Records a CPU usage data point, discarding oldest over capacity.
     * @param {number} percent - CPU usage percentage.
     * @private
     * @returns {void}
     */
    #recordCpuSample(percent) {
        this.#cpuSamples.push(percent);
        if (this.#cpuSamples.length > this.#samplingCount) {
            this.#cpuSamples.shift();
        }
    }

    /**
     * Records a RAM usage data point, discarding oldest over capacity.
     * @param {number} percent - RAM usage percentage.
     * @private
     * @returns {void}
     */
    #recordMemSample(percent) {
        this.#memSamples.push(percent);
        if (this.#memSamples.length > this.#samplingCount) {
            this.#memSamples.shift();
        }
    }

    /**
     * Calculates a weighted average for given samples array.
     * @param {number[]} samples - Array of usage percentages.
     * @private
     * @returns {number} Weighted usage percentage.
     */
    #calculateWeightedAverage(samples) {
        if (samples.length === 0) return 0;
        const weighted = samples.reduce((accumulator, val, idx) => ({
            sum: accumulator.sum + val * (idx + 1),
            weight: accumulator.weight + (idx + 1)
        }), {
            sum: 0,
            weight: 0
        });
        return weighted.sum / weighted.weight;
    }

    /**
     * Applies hysteresis: decrease if CPU>=upperCpu or RAM>=upperMem;
     * increase if CPU<lowerCpu and RAM<lowerMem.
     * @param {number} avgCpu - Weighted CPU average.
     * @param {number} avgMem - Weighted RAM average.
     * @private
     * @returns {void}
     */
    #adjustConcurrencyIfNeeded(avgCpu, avgMem) {
        if (avgCpu >= this.#upperCpuThresholdPercent || avgMem >= this.#upperMemThresholdPercent) {
            this.#changeConcurrency(GlobalResource.DECREASE);
        } else if (avgCpu < this.#lowerCpuThresholdPercent && avgMem < this.#lowerMemThresholdPercent) {
            this.#changeConcurrency(GlobalResource.INCREASE);
        }
    }

    /**
     * Adjusts the concurrency limit up or down by doubling or halving,
     * clamped between minConcurrency and maxConcurrency.
     * @param {number} direction - Use GlobalResource.INCREASE or DECREASE.
     * @private
     * @returns {void}
     */
    #changeConcurrency(direction) {
        const oldValue = this.#currentConcurrency;
        let newValue = oldValue;
        switch (direction) {
            case GlobalResource.DECREASE:
                if (oldValue > this.#minConcurrency) newValue = Math.max(this.#minConcurrency, Math.floor(oldValue / 2));
                break;
            case GlobalResource.INCREASE:
                if (oldValue < this.#maxConcurrency) newValue = Math.min(this.#maxConcurrency, oldValue * 2);
                break;
            default:
                return;
        }
        if (newValue !== oldValue) {
            this.#applyNewConcurrency(newValue, direction, oldValue);
        }
    }

    /**
     * Updates concurrency, logs transition, and notifies processors.
     * @param {number} newValue - New concurrency limit.
     * @param {number} direction - INCREASE or DECREASE flag.
     * @param {number} oldValue - Previous concurrency limit.
     * @private
     * @returns {void}
     */
    #applyNewConcurrency(newValue, direction, oldValue) {
        this.#currentConcurrency = newValue;
        this.#logger(`Concurrency ${direction}: ${oldValue} → ${newValue}`, 'global-resource');
        for (const proc of this.#processors.values()) {
            proc.setConcurrency(newValue);
        }
    }
}

module.exports = GlobalResource;