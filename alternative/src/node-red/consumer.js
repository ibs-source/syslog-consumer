/**
 * @module redis-syslog-consumer
 * @description
 * Node-RED node definition that reads from a Redis stream using consumer groups,
 * emits each entry as a message, and performs XACK/XDEL only after downstream ack.
 */

const { streamProcessors } = require('../shared');
const StreamProcessor = require('../core/stream-processor');

module.exports = function (RED) {
  /**
   * Node-RED node constructor for the redis-syslog-consumer node.
   *
   * @param {Object} config - Standard Node-RED node configuration object.
   */
  function RedisSyslogConsumerNode(config) {
    RED.nodes.createNode(this, config);
    this.url = config.url;
    this.stream = config.stream;
    this.resourceConfigurationId = config.resource;
    const resourceNode = RED.nodes.getNode(this.resourceConfigurationId);
    if (typeof resourceNode !== 'object' || !resourceNode.resource) {
      this.error('Resource Manager node not found');
      return;
    }
    const resource = resourceNode.resource;
    const status = (fill, shape, text) => {
      const Status = {};
      Status.fill = fill;
      Status.shape = shape;
      Status.text = text || '<not-set>';
      this.status(Status);
    }
    const output = (message) => {
      const Message = [];
      Message.push({
        payload: message
      });
      Message.push(null);
      this.send(Message);
    }
    const log = (message, context) => {
      const Message = [];
      Message.push(null);
      Message.push({
        payload: message,
        context: context || 'syslog'
      });
      this.send(Message);
    };
    const processor = new StreamProcessor(this.url, this.stream, resource.concurrency, status, output, log);
    streamProcessors.set(this.stream, processor);
    processor.initialize().then(() => {
      resource.start();
    }).catch((err) => {
      this.error(`Initialization error: ${err.message}`)
    });

    this.on('input', async (message, send, done) => {
      const redis = message?.payload;
      try {
        if (typeof redis.id !== 'string'
          || redis.ack !== true) return done();
        await processor.acknowledgeAndDelete(redis.id);
      } catch (err) {
        const id = redis?.id || 'unknown';
        this.error(`Acknowledgement and Delete failed for message id: "${id}". Error: ${err.message}`);
      }
      done();
    });

    this.on('close', async (done) => {
      try {
        const processor = streamProcessors.get(this.stream);
        if (processor) {
          // Wait for the processor to shut down
          await processor.shutdown();
          streamProcessors.delete(this.stream);
        }
        resource.stop();
        // Signal to Node-RED that the closure is complete
        done();
      } catch (err) {
        this.error(`Error during shutdown: ${err.message}`);
        done();
      }
    });
  }

  RED.nodes.registerType('redis-syslog-consumer', RedisSyslogConsumerNode);
};
