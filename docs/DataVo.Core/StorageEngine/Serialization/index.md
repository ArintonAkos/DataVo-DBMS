# Serialization Overview

The `Serialization` module acts as the bridge between dynamically-typed C# runtime objects (such as `Dictionary<string, dynamic>`) and raw binary byte arrays. This module natively supports structuring, encoding, and interpreting structured data so it can be safely stored on disk or held efficiently in memory.

## Core Responsibilities
* **Binary Processing:** Translates native C# types (strings, integers, floats, booleans) into precise byte sequences and vice-versa.
* **Schema Enforcement:** Applies database catalog schema definitions during serialization, ensuring that raw values fit within the designated column size limits and type constraints.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `RowSerializer.cs` | The central utility class. It computes exact byte offsets, dynamically pads or truncates strings based on column capacities, translates types safely, and serializes/deserializes data streams in predictable, fixed-length formats. |

## Dependencies & Interactions
This module is crucial for both `DiskStorageEngine.cs` and `InMemoryStorageEngine.cs`. By standardizing the binary output format, the serializer allows the core engine to safely decouple high-level logic representations (`Row` objects) from physical byte manipulations.
