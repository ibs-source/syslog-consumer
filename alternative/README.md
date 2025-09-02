# redis-syslog-consumer

## ⚙️ Prerequisites

- **Node.js** v14 or higher
- **Redis** v5.0 or higher (Streams support required)
- A reachable Redis server from your environment

A reusable library for consuming Redis streams reliably using consumer groups, with back‑pressure, retry, and dynamic concurrency control based on system load. Can be used as a standalone library or as a Node-RED node.

---

## 📦 Installation

Add the package to your project:

```bash
npm install redis-syslog-consumer
```

If using Node-RED, restart your Node-RED instance to load the new nodes.

---

## 🚀 Usage

### Standalone Usage

```javascript
const { StreamProcessor, GlobalResource } = require("redis-syslog-consumer");

// Create a map to store processor instances
const processors = new Map();

// Create a resource manager
const resourceManager = new GlobalResource(processors, {
  maxConcurrency: 10,
  minConcurrency: 1,
});

// Start the resource manager
resourceManager.start();

// Message handler function
const handleMessage = (message) => {
  console.log("Received message:", message.payload);
  console.log("Redis ID:", message._redisId);

  // Process the message...

  // After processing, acknowledge the message
  streamProcessor.acknowledgeAndDelete(message._redisId);
};

// Create a stream processor
const streamProcessor = new StreamProcessor(
  "redis://localhost:6379",
  "syslog",
  resourceManager.concurrency,
  handleMessage
);

// Register with resource manager
processors.set("syslog", streamProcessor);

// Initialize the stream processor
streamProcessor.initialize();

// Cleanup on shutdown
process.on("SIGINT", () => {
  processors.delete("syslog");
  resourceManager.stop();
  process.exit(0);
});
```

### Node-RED Usage

Once installed, you'll have two new nodes in the palette under **Syslog** and **config**:

1. **Redis Syslog Consumer** (`redis-syslog-consumer`)
2. **Resource Manager** (`redis-syslog-resource-manager`)

#### Redis Syslog Consumer Node

- **Inputs**: 1
- **Outputs**: 2 (output, logs)

##### Node Properties

| Property             | Type   | Description                                      |
| -------------------- | ------ | ------------------------------------------------ |
| **Name**             | String | Optional label                                   |
| **Redis URL**        | String | Connection URI (`redis://host:port`)             |
| **Stream Key**       | String | Redis stream name (e.g. `syslog`)                |
| **Resource Manager** | Config | Select your `redis-syslog-resource-manager` node |

##### Behavior

1. On deploy, it creates a `StreamProcessor` with:
   - A Redis client connection
   - A consumer‐group named `grp:<stream>`
   - A random consumer name `name:<id>`
2. **Initialization**:
   - Creates or reuses the Redis consumer group
   - Drains existing backlog (XREADGROUP from `0`)
   - Claims stale pending entries (older than 5 minutes)
   - Starts a continuous poll (BLOCK 30s) for new messages
3. **Message flow**:
   - Each entry becomes a Node-RED message payload (`msg.payload`) with metadata (`msg._redisId`).
   - The developer must re-inject an object `{ _redisId, _redisAck: true }` into the node to ACK/DEL from the stream.
4. **Logging**: any lifecycle events, errors, or debug can be emitted to the second output.

#### Resource Manager Config Node

Global config node that monitors CPU and RAM and adjusts all active **Redis Syslog Consumer** nodes' concurrency based on hysteresis thresholds.

##### Config Properties

| Property                    | Default | Description                             |
| --------------------------- | ------- | --------------------------------------- |
| **Max Concurrency**         | `10`    | Upper bound for message‐parallelism     |
| **Min Concurrency**         | `1`     | Lower bound for parallelism             |
| **Sampling Size**           | `10`    | Number of samples in sliding window     |
| **Sampling Interval (ms)**  | `5000`  | Time between CPU/RAM checks             |
| **Upper CPU Threshold (%)** | `80`    | CPU% above which concurrency is halved  |
| **Lower CPU Threshold (%)** | `30`    | CPU% below which concurrency is doubled |
| **Upper RAM Threshold (%)** | `80`    | RAM% above which concurrency is halved  |
| **Lower RAM Threshold (%)** | `30`    | RAM% below which concurrency is doubled |

##### Behavior

- Uses `pidusage` to sample process CPU and memory.
- Maintains sliding‐window averages with weighted samples.
- Applies hysteresis:
  - **Decrease** concurrency if CPU ≥ upper CPU threshold **or** RAM ≥ upper RAM threshold.
  - **Increase** concurrency if both CPU < lower CPU threshold **and** RAM < lower RAM threshold.
- Doubles or halves the global concurrency, clamped between min/max.

---

## 📖 API Reference

### `StreamProcessor`

```js
const { StreamProcessor } = require("redis-syslog-consumer");
```

#### Constructor

```ts
new StreamProcessor(
  url: string,
  stream: string,
  concurrency: number,
  onMessageCallback: (msg: { payload: string; _redisId: string }) => void,
  options?: {
    logger?: (message: string, context?: string) => void,
    status?: (fill: string, shape: string, text: string) => void,
    consumerName?: string,
    consumerGroupPrefix?: string
  }
)
```

- **`url`**: Redis connection URI
- **`stream`**: Redis stream key
- **`concurrency`**: max parallel messages
- **`onMessageCallback`**: invoked with each parsed message
- **`options`**: Optional configuration

#### Methods

- `initialize(): Promise<void>` – start consuming (create group, drain backlog, poll).
- `acknowledgeAndDelete(messageId: string): Promise<void>` – XACK + XDEL for a given message.
- `setConcurrency(newConcurrency: number)` – dynamically adjust concurrency.

### `GlobalResource`

```js
const { GlobalResource } = require("redis-syslog-consumer");
```

#### Constructor

```ts
new GlobalResource(
  processors: Map<string, { setConcurrency: (number) => void }>,
  config?: {
    maxConcurrency?: number,
    minConcurrency?: number,
    samplingCount?: number,
    samplingIntervalMs?: number,
    upperCpuThresholdPercent?: number,
    lowerCpuThresholdPercent?: number,
    upperMemThresholdPercent?: number,
    lowerMemThresholdPercent?: number
  },
  logger?: (message: string, context?: string) => void
)
```

#### Methods

- `start()` - Start monitoring CPU and RAM usage
- `stop()` - Stop monitoring
- `registerProcessor(streamKey: string, processor: object)` - Add a processor
- `unregisterProcessor(streamKey: string)` - Remove a processor
- `get concurrency()` - Get current concurrency value

---

## 🛠 Example Node-RED Flow

```json
[
  {
    "id": "1",
    "type": "redis-syslog-resource-manager",
    "name": "ResourceManager",
    "concurrencyMax": 20,
    "concurrencyMin": 1,
    "samplingCount": 15,
    "samplingInterval": 3000,
    "thresholdUpper": 75,
    "thresholdLower": 25,
    "thresholdUpperMem": 75,
    "thresholdLowerMem": 25
  },
  {
    "id": "2",
    "type": "redis-syslog-consumer",
    "stream": "syslog",
    "url": "redis://127.0.0.1:6379",
    "resource": "1",
    "wires": [["3"], ["4"]]
  }
]
```

Inject back to acknowledge:

```json
{ "payload": { "redis": { "id": "1680000000000-0", "ack": true } } }
```

---

## Contributing

Contributions are welcome! Please fork the repository, create a feature branch, and submit a pull request with a detailed explanation of changes.

---

## Versioning

We use [SemVer](https://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/ibs-source/syslog/tags).

---

## Authors

- **Paolo Fabris** - _Initial work_ - [ibs.srl](https://www.ibs.srl/)

See also the list of [contributors](https://github.com/ibs-source/syslog/blob/main/CONTRIBUTORS.md) who participated in this project.

---

## ⚖️ License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
