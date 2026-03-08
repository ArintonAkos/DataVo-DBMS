# Binary B-Tree Overview

The `Binary` submodule contains a classic disk-backed B-Tree implementation. Unlike the B+Tree variant, this structure stores key/value mappings inside regular B-Tree pages rather than keeping all row IDs only at the leaf level.

## Files in this folder

| File | Purpose |
| :--- | :--- |
| `BinaryBTreeIndex.cs` | High-level `IIndex` implementation for insert, search, logical delete, and row-ID existence checks. |
| `BTreePage.cs` | Fixed-size 4 KB page model containing keys, row IDs, and child page pointers. |
| `DiskPager.cs` | Low-level page allocator and memory-mapped file accessor for B-Tree pages. |

## How it works

- The pager reserves page `0` for metadata.
- Tree pages begin at page `1`.
- Each page stores a bounded number of string keys and aligned row ID values.
- Insertion descends recursively until it reaches a non-full page.
- If a child is full during descent, it is split before recursion continues.

## Deletion behavior

The current implementation does **not** rebalance or compact the tree during deletion. Instead, `DeleteValues` scans pages and replaces matching row IDs with the sentinel value `0`.

That means:

- the physical tree structure remains unchanged,
- deleted entries are ignored by read operations,
- the file may retain tombstoned slots until the index is rebuilt.

## Strengths and limitations

| Aspect | Notes |
| :--- | :--- |
| Exact-match lookups | Supported. |
| Page splitting on insert | Supported. |
| Memory-mapped file I/O | Supported. |
| Logical deletion via tombstones | Supported. |
| Range-scan optimization | Limited compared to the B+Tree because values are not organized exclusively in linked leaf pages. |
