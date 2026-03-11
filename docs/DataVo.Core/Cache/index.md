# Cache Overview

The `Cache` module acts as the intermediate in-memory buffering layer for localized data transients within the `DataVo.Core` architecture. It accelerates repeated read operations and minimizes raw disk I/O by temporarily retaining frequently accessed payloads during a session lifecycle.

## Core Responsibilities

- **In-Memory Buffering:** Stores recently fetched elements or execution contexts to eliminate redundant disk seek patterns.
- **State Encapsulation:** Provides a decoupled thread-safe mapping mechanism to track objects actively accessed by query transactions.

## Component Breakdown

| Component (File)  | Architectural Role                                                                                                                    |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `CacheStorage.cs` | Implements the primary concurrent key-value storage dictionary responsible for housing transient caching data during query execution. |

## File Documentation

- [CacheStorage](./CacheStorage.md)

## Dependencies & Interactions

The `Cache` module is natively consumed by the `StorageEngine` to bypass physical disk loading when attempting to read previously requested pages or metadata. It works passively behind the abstraction layers to improve system throughput across sequential query evaluations without dictating business logic.
