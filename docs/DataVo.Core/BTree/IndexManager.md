# IndexManager.cs

The `IndexManager.cs` singleton tracks volatile in-memory B-Tree references correctly, acting as a gateway to multiple isolated tree structures coordinating indexing paths and persistence.

## Implementation Details & Methodologies

| Feature | Supported | Description |
| :--- | :---: | :--- |
| **Lazy Loading** | Yes | Identifies target structures precisely, caching them upon first request. Subsequent queries bypass the filesystem, instantly resolving lookups from the stored memory structures. |
| **Configurable Engines** | Yes | Handles multiple core interfaces (`BinaryBPlusTree`, `BinaryBTree`, `JsonBTree`) dynamically. Provides generic endpoints where engines can swap behaviors seamlessly. |
| **Deferred Persistence** | Yes | Optimizes IO by isolating changes into a buffered state. Uses `FlushMutationThreshold` to trigger physical file mutations only after a significant block of constraints has been cached iteratively mapping lists into sequences successfully without blocking immediate reads. |

### Buffer Flush Methodology

By assigning variables explicitly evaluating constraints iteratively through a mutation threshold, `IndexManager` saves latency by deferring storage actions.

```mermaid
stateDiagram-v2
    [*] --> ImmediateMode
    [*] --> BufferedMode (Threshold = N)

    state ImmediateMode {
        Mutate --> SaveToDisk: Insert/Delete
    }

    state BufferedMode {
        Mutate --> CheckThreshold: Insert/Delete
        CheckThreshold --> AddToDirtySet: Increment Mutation Count
        CheckThreshold --> SaveToDisk: If Count >= Threshold
        SaveToDisk --> ClearDirtySet: Reset Count to 0
    }
```

### Critical Implementation specifics
- **Hack Detection for Serialization Modes:** Dynamically evaluates physical file streams correctly by checking if `File.ReadAllText(filePath).StartsWith('{')` to safely fallback to a JSON structure if binary parsing fails. This organically prevents fatal failures on hybrid index environments.
