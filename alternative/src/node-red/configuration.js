/**
 * @module redis-syslog-resource-manager/configuration
 * @description
 * Node-RED config node that sets up a GlobalResource:
 * - Periodically samples CPU and RAM usage and adjusts global concurrency with hysteresis.
 * - Exposes start/stop controls for the manager via the Node-RED editor.
 */

const { streamProcessors } = require('../shared');
const GlobalResource = require('../core/resource-manager');

let globalResource = null;

module.exports = function (RED) {
  /**
   * Node-RED config node constructor for object.
   *
   * @param {Object} object - Standard Node-RED config object.
   */
  function ConfigurationNode(object) {
    RED.nodes.createNode(this, object);
    if (null === globalResource) {
      const log = (message, context) => {
        if (streamProcessors.size === 0) return console.log(`[${context}] ${message}`);
        for (const processor of streamProcessors.values()) processor.log(message, context);
      };
      globalResource = new GlobalResource(streamProcessors, object, log);
    }
    this.resource = globalResource;
  }

  RED.nodes.registerType('redis-syslog-resource-manager', ConfigurationNode, {
    category: 'config'
  });
};