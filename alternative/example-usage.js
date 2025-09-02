/**
 * Example of using the redis-syslog-consumer library without Node-RED
 */

const StreamProcessor = require('./src/core/stream-processor');
const GlobalResource = require('./src/core/resource-manager');
const { streamProcessors } = require('./src/shared');

// Create a resource manager
const log = (message, context) => {
    console.log(`[${context}] ${message}`);
};

// Status callback
const status = (fill, shape, text) => {
    console.log(`Status: ${fill} ${shape} ${text}`);
};

// Message callback
const handleMessage = (message) => {
    console.log('Received message:', message.message);
    console.log('Redis ID:', message.redis.id);

    // Process the message here...

    // After successfully processing, acknowledge the message
    setTimeout(() => {
        processor.acknowledgeAndDelete(message.redis.id)
            .then(() => console.log('Message acknowledged and deleted:', message.redis.id))
            .catch(err => console.error('Failed to acknowledge message:', err));
    }, 1000); // Simulate async processing
};

// Create resource manager with configuration
const resourceManager = new GlobalResource(streamProcessors, {
    maxConcurrency: 10,
    minConcurrency: 1,
    samplingCount: 15,
    samplingIntervalMs: 3000,
    upperCpuThresholdPercent: 80,
    lowerCpuThresholdPercent: 30,
    upperMemThresholdPercent: 80,
    lowerMemThresholdPercent: 30
}, log);

// Start the resource manager
resourceManager.start();

// Create a stream processor for a specific stream
const processor = new StreamProcessor(
    'redis://localhost:6379',
    'syslog',
    resourceManager.concurrency,
    status,
    handleMessage,
    log
);

// Add the processor to the shared map
streamProcessors.set('syslog', processor);

// Initialize the stream processor
processor.initialize()
    .then(() => console.log('Stream processor initialized'))
    .catch(err => console.error('Failed to initialize stream processor:', err));

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('Shutting down...');
    streamProcessors.delete('syslog');
    resourceManager.stop();
    process.exit(0);
});