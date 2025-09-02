# internal/logger

Thin Logrus adapter that implements the `ports.Logger` interface. Provides leveled logging, structured fields, and consistent formatting across the application. Supports JSON and text output with ISO-like timestamps and maps the common field keys (timestamp, level, message).

Scope

- Create a configured Logrus logger (level and format).
- Provide structured logging methods via `ports.Logger`: Trace, Debug, Info, Warn, Error, Fatal.
- Provide `WithFields` to attach context fields immutably.
- Offer convenience helpers to build typed fields (String, Int, Float64, Bool, Any, Error).

Key Behavior

- Log level is configured at startup from App config (env/flags).
- JSON formatter uses consistent field names: timestamp, level, message.
- Output is directed to stdout; caller reporting is disabled for performance and clean logs.

API Surface (selected)

- NewLogrusLogger(level, format) (\*LogrusLogger, error)
- InitGlobalLogger(level, format) error
- GetGlobalLogger() ports.Logger
- Methods on `ports.Logger`:
  - Trace/Debug/Info/Warn/Error/Fatal(msg string, fields ...ports.Field)
  - WithFields(fields ...ports.Field) ports.Logger

Configuration (ENV, Flags, Description)
Precedence: defaults → environment variables → CLI flags.

App & Logging
| ENV | Flag | Default | Description |
|---|---|---|---|
| LOG_LEVEL | --log-level | info | Log level: trace, debug, info, warn, error, fatal, panic. |
| LOG_FORMAT | --log-format | json | Log format: json or text. |

Usage

```go
log, _ := logger.NewLogrusLogger("debug", "json")
// With contextual fields:
reqLog := log.WithFields(ports.Field{Key:"component", Value:"processor"})
reqLog.Info("starting component")

// Field helpers:
reqLog.Error("operation failed", logger.Error(err), logger.String("op", "publish"))
```
