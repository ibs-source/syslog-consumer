/**
 * @module shared
 * @description Shared in-memory storage for StreamProcessor instances.
 */
module.exports = {
    /**
     * Map of active StreamProcessor instances by stream key.
     * @type {Map<string, Object>}
     */
    streamProcessors: new Map()
};