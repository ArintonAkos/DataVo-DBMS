# B+Tree Overview

The `BPlus` submodule contains the binary B+Tree implementation used as the default index engine. In this design, internal pages hold routing keys while leaf pages hold the actual row IDs. Leaf pages are linked together, which makes sequential traversal straightforward.

## Files in this folder

| File | Purpose |
| :--- | :--- |
| `BinaryBPlusTreeIndex.cs` | Main `IIndex` implementation for insert, search, logical delete, and row-ID existence checks. |
| `BPlusTreePage.cs` | Fixed-size page model used for both internal and leaf pages. |
| `BPlusDiskPager.cs` | Memory-mapped file pager responsible for page allocation, metadata, reads, and writes. |

## Page model

- Page `0` stores metadata.
- Internal pages store encoded keys and child page IDs.
- Leaf pages store encoded keys and row IDs.
- Leaf pages are connected through `NextPageId`.
- Keys are stored as fixed 32-byte arrays produced by `IndexKeyEncoder`.

## Search behavior

1. Encode the logical key with `IndexKeyEncoder.Encode`.
2. Descend from the root through internal routing pages.
3. Reach the first leaf page that could contain the key.
4. Scan forward through linked leaf pages until keys are greater than the target.

This makes exact-match lookups efficient while also supporting sequential leaf traversal.

## Insert behavior

- Inserts descend recursively until a non-full page is reached.
- If a child page is full, it is split before insertion continues.
- Leaf splits keep the promoted routing key in the parent while preserving the actual row IDs in leaf pages.

## Delete behavior

`DeleteValues` performs logical deletion by replacing matching row IDs with `0` in leaf pages.

The current implementation does not perform merge or redistribution steps after deletion, so:

- tombstoned slots remain until a rebuild,
- search ignores row ID `0`,
- write complexity stays lower than full B+Tree delete rebalancing.

## Why this implementation is the default

The B+Tree layout fits the current query engine well because:

- equality predicates use exact-match lookup,
- sequential leaf layout is a good foundation for future range access,
- binary fixed-size pages avoid JSON serialization overhead,
- the pager supports efficient reuse through memory-mapped files.
