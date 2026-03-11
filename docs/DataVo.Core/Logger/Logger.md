# Logger

`Logger` is the low-friction diagnostic output utility used throughout the engine.

## Supported operations

- `Info(string)`
- `Error(string)`

## Output format

Each message is prefixed with a timestamp such as:

```text
[11/03/26 16:40:12:101] Error: message text
```

## Why it remains simple

The logger is intentionally lightweight so it can be used safely in startup, recovery, parser debugging, and storage internals without bringing in a larger logging abstraction.
