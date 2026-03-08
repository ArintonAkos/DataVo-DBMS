# Memory (StorageEngine) Overview

The `Memory` module encapsulates the `InMemoryStorageEngine`, providing a fully functional, highly volatile storage implementation that mirrors exactly the behavior of the persistent `DiskStorageEngine`. 

## Core Responsibilities
* **Volatile Isolation:** Implements database mutations (Insert, Update, Delete) bypassing active filesystem handles replacing them entirely utilizing high-speed internal RAM arrays natively structuring records.
* **Testing & Integrity:** Supplies testing environments ensuring robust evaluation without bloating internal hard drives by quickly instantiating logical data structures in RAM.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `InMemoryStorageEngine.cs` | Replicates standard `IStorageEngine` methods using a `ConcurrentDictionary<string, List<byte[]>>` to store table definitions and serialized raw data as fast, linear memory arrays. |

## Dependencies & Interactions
This engine binds natively via `DataVoContext.cs` as the designated `.Storage` provider when the user specifies volatile instantiation. It serializes properties via the `Serialization` module cleanly and assigns RowIDs securely utilizing organic List index tracking rather than physical file offset coordination limits.

## Implementation Specifics
* **Capabilities Supported:** Mimics complete database operations (`SELECT, INSERT, UPDATE, DELETE`), storing records, and mapping index offsets optimally.
* **Limitations Not Supported:** Cannot permanently process data to disk. Data is completely discarded upon application exit or volatile scope termination.
