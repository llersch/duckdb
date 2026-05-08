# Headless Duck — a standalone DuckDB-native columnar file format

**Status:** proof-of-concept, not production.
**Authors:** Lucas Lersch (MotherDuck), built in a guided session.
**Audience:** MotherDuck internal, DuckLake team, upstream DuckDB maintainers.

## 1. Motivation

Data-lake table formats (Iceberg, Delta, [DuckLake](https://ducklake.select)) currently assume one data file format in practice: Apache Parquet. DuckDB's own storage engine has encoding and compression work that Parquet does not — FSST, ALP / ALP-RD, Chimp, Roaring — and that work is the reason DuckDB's `.duckdb` files are often smaller and faster to scan than the equivalent Parquet.

The thesis of this PoC: a stripped-down, read-oriented DuckDB storage file — "headless duck" — can be produced as a standalone artifact, live inside a data-lake table alongside Parquet files, and be read by DuckDB without any catalog or attached database. If viable, it becomes a real option for DuckLake and eventually a candidate for Iceberg/Delta as a second data-file format.

This document describes what the PoC implements, what it proves, and what it does not.

## 2. What this PoC demonstrates

- **A DuckDB extension** (`headless_duck`) that registers:
  - `COPY tbl TO 'f.hduck' (FORMAT headless_duck)` for writing.
  - `read_headlessduck('f.hduck')` table function for reading.
- **End-to-end round-trip through DuckDB's real storage engine.** Writes go through `RowGroupCollection` → `RowGroup::WriteToDisk` → `ColumnDataCheckpointer` → compression codec → block manager → file. Reads go through `RowGroupCollection::Initialize(PersistentCollectionData)` → `TableScanState` → `CollectionScanState::Scan`.
- **No catalog at read time.** The reader is given a file path. It constructs everything else from the file's contents. No external schema, no table name, no transaction, no attached database-of-origin needed.
- **Size-competitive with Parquet on an early test.** On 100k rows of `(INTEGER, VARCHAR)` where the VARCHAR has a repeating prefix, `.hduck` was 786 KB versus Parquet's 888 KB. This is FSST on the VARCHAR doing what FSST does; other shapes will differ. TPC-H at scale is the next benchmark.

## 3. File layout

```
+-------------------------------------------+ offset 0
| HeadlessDuckHeader   (16 bytes)           |
|   char  magic[8]       = "HDUCK1\0\0"     |
|   uint32 format_version = 2               |
|   uint32 flags          = 0               |
+-------------------------------------------+ offset sizeof(header) = 16
| Block 0   (block_alloc_size bytes)        |
| Block 1                                   |   <- data blocks (column segments)
| ...                                       |      and metadata blocks
| Block N-1                                 |      (interleaved, all sequential)
+-------------------------------------------+ offset 16 + N * alloc_size
| Metadata section (variable)               |
|   schema (column names + LogicalTypes)    |
|   row_group_pointers (list)               |
|   MetadataManager state blob              |
+-------------------------------------------+
| HeadlessDuckFooter   (56 bytes)           |
|   uint64 metadata_offset                  |
|   uint64 metadata_length                  |
|   uint64 metadata_checksum  (reserved, 0) |
|   uint64 block_count                      |
|   uint64 block_alloc_size                 |
|   uint64 meta_block_root                  |
|   char   magic[8]       = "HDUCK1\0\0"    |
+-------------------------------------------+ EOF
```

Blocks live at `offset = sizeof(header) + block_id * block_alloc_size`. `block_id` is just an index into the file — there is no free list, no checkpoint rotation. The file is immutable once finalized.

The metadata section is a `BinarySerializer`-encoded object with the following fields:

| field_id | name                    | type                                    |
|---------:|-------------------------|-----------------------------------------|
| 100      | `column_count`          | `uint32`                                |
| 101      | `columns`               | list of `{name: string, type: LogicalType}` |
| 103      | `row_groups`            | list of `RowGroupPointer`               |
| 104      | `metadata_manager_size` | `uint64`                                |
| 105      | `metadata_manager_bytes`| raw bytes (see below)                   |

Field 102 is reserved — it was used by an earlier step-11 prototype that stored a naive `ColumnDataCollection::Serialize` blob. It is no longer written. Readers should ignore missing field 102.

`metadata_manager_bytes` is the output of `MetadataManager::Write(stream)` — a compact representation of the block-map so the reader can reconstruct the `MetadataManager` before pinning any `MetaBlockPointer`. See `src/storage/metadata/metadata_manager.cpp` for the exact serialization.

## 4. Architecture

### 4.1 `HeadlessDuckBlockManager` (`headless_duck_block_manager.{hpp,cpp}`)

A minimal `BlockManager` subclass. Every mutability-related method is a no-op or a throw; the file is append-only at write time and read-only at read time. In particular:

- `GetFreeBlockId()` returns a monotonically increasing counter (`next_block_id++`).
- `Write(FileBuffer &, block_id_t)` writes at `base_offset + id * block_alloc_size`.
- `Read(Block &)` reads at the same offset.
- `WriteHeader(DatabaseHeader)` is **intentionally empty** — we do not use DuckDB's database header; our format has its own footer, written by the COPY callback after checkpoint is complete.
- `MarkBlockAsModified(...)` throws — we never rewrite a block.

`MetadataManager` is inherited intact from the base class. Its state (the `blocks` map) is persisted into the file's metadata section and restored on read.

### 4.2 `HeadlessDuckTableIOManager` (inline in the header)

Trivial. Returns the `HeadlessDuckBlockManager` for both `GetIndexBlockManager()` and `GetBlockManagerForRowData()`, returns its `MetadataManager`, and hardcodes `GetRowGroupSize() == DEFAULT_ROW_GROUP_SIZE`. Modeled on `SingleFileTableIOManager`.

### 4.3 Write pipeline (`headless_duck_writer.cpp`)

```
CopyFunction callback          Action
-----------------------------  ------------------------------------------------
InitializeGlobal               - Open file, write leading header
                               - Open a second file handle for the block manager
                               - Construct: HeadlessDuckBlockManager (WRITE),
                                            HeadlessDuckTableIOManager,
                                            DataTableInfo,
                                            RowGroupCollection (empty)
                               - Construct a staging ColumnDataCollection (see §6.3)
InitializeLocal                - Per-thread state (currently empty)
Sink                           - Append chunk to the global staging CDC
Combine                        - No-op
Finalize                       - Replay staged chunks into RowGroupCollection
                                 via Append + FinalizeAppend
                               - For each row group: RowGroup::WriteToDisk(info)
                                 with RowGroupWriteInfo(PartialBlockManager,
                                 compression_types={COMPRESSION_AUTO, ...})
                               - FlushPartialBlocks
                               - For each ColumnCheckpointState:
                                   record MetaBlockPointer,
                                   serialize PersistentColumnData via MetadataWriter
                               - Flush MetadataManager (writes meta blocks to disk)
                               - Serialize metadata section (schema + row_group_pointers
                                 + MetadataManager::Write bytes)
                               - Write metadata section at offset 16 + block_count * alloc_size
                               - Write footer
```

The crucial piece that avoids the catalog-coupling wall: we do not use `RowGroupCollection::Checkpoint(TableDataWriter&, ...)`. `TableDataWriter` requires a `TableCatalogEntry`. Instead we call the lower-level `RowGroup::WriteToDisk(RowGroupWriteInfo&)` per row group, which only needs a `PartialBlockManager` + compression settings, and then replicate the metadata-serialization step from `RowGroup::Checkpoint` inline.

### 4.4 Read pipeline (`headless_duck_reader.cpp`)

```
TableFunction callback         Action
-----------------------------  ------------------------------------------------
Bind                           - Open file, validate header + footer magics
                               - Read metadata section, deserialize:
                                   schema (names + LogicalTypes),
                                   row_group_pointers,
                                   MetadataManager state blob
                               - Construct: HeadlessDuckBlockManager (READ),
                                            HeadlessDuckTableIOManager,
                                            DataTableInfo,
                                            RowGroupCollection (empty)
                               - Restore MetadataManager state
                                 (populates its internal block map)
                               - For each row group pointer:
                                   For each data_pointer:
                                     MetadataReader at that pointer,
                                     BinaryDeserializer (with LogicalType set),
                                     PersistentColumnData::Deserialize
                                   Build PersistentRowGroupData
                               - Wrap in PersistentCollectionData,
                                 collection.Initialize(pcd)
                               - Return schema as return_types / names
InitGlobal                     - Create TableScanState
                               - state->Initialize(column_ids, &context, nullptr, nullptr)
                               - collection.InitializeScan(
                                     QueryContext(context),
                                     state->table_state,
                                     column_ids,
                                     nullptr)
Scan                           - state.table_state.Scan(
                                     output,
                                     TableScanType::TABLE_SCAN_COMMITTED_ROWS)
```

### 4.5 "Borrowed" `AttachedDatabase`

`DataTableInfo` requires an `AttachedDatabase &`. We borrow whatever the user's default database is (`DatabaseManager::GetDefaultDatabase(context)`). In practice this reference is used for:

- Allocator access (`Allocator::Get(info->GetDB())`).
- Empty `TableIndexList` that we never populate.
- Diagnostic strings in error messages.

It is **not** used for:
- Catalog dispatch (we never look up our synthetic table in any catalog).
- Transaction coordination (we use `TransactionData::Committed()`).
- Block management (our own block manager takes over).

This works fine today but is a philosophical wart — a headless file should not require *any* attached database. See §6.1 for the upstream proposal to decouple.

## 5. Benchmarks (preliminary)

| Dataset                                   | `.hduck` | `.parquet` | ratio |
|-------------------------------------------|---------:|-----------:|------:|
| `range(100000)` + `'row_' \|\| range`     | 786 KB   | 888 KB     | 0.89× |

The VARCHAR column has a heavily shared prefix, which is exactly where FSST excels. On less-prefix-heavy strings, on numeric columns without tight zonemaps, or on wide rows, the picture will differ. **TPC-H `lineitem` is the real benchmark** and is the next item on the roadmap.

Caveat: block-size alignment. Our files round up to multiples of 256 KB (`DEFAULT_BLOCK_ALLOC_SIZE`). At TPC-H scale this is noise; at sub-megabyte scale it dominates. A small PoC file is typically 3-4 blocks (~1 MB) no matter how little data it contains.

## 6. Known limitations and sharp edges

### 6.1 Still requires an `AttachedDatabase` at read time

`DataTableInfo`'s constructor requires one. We borrow the user's default DB. This is fine in practice — any reader already has a `ClientContext` — but it means a purely standalone reader (no DuckDB instance attached to anything) is not currently possible.

The upstream change that would fix this cleanly: split `DataTableInfo` so the parts we actually need (types, allocator source, I/O manager) can be constructed without an `AttachedDatabase`. Small, well-scoped refactor; happy to prototype.

### 6.2 `meta_block_root` is approximate

The footer field `meta_block_root` is currently hardcoded to `1` (i.e. "the block right after the data block") for files with more than one block, and `0` otherwise. This works for single-row-group files but is not the correct general invariant. The `MetadataManager` state blob contains the actual block map, which is the source of truth on read — `meta_block_root` is not consulted by the current reader. The field should either be removed or given a precise definition in v3 of the format.

### 6.3 Write-side append is serialized through a global lock

Sink appends directly into the `RowGroupCollection` under a mutex. This removes the earlier staging `ColumnDataCollection`, but the append/finalize path still wants stress testing under parallel COPY. If the global lock becomes a bottleneck, the next step is a per-thread append design with explicit combine-time ordering semantics.

### 6.4 Lightly tested / not tested

- Multi-row-group files. The current sqllogictests cover empty files, small files, and files spanning multiple vectors, but not yet data that crosses `DEFAULT_ROW_GROUP_SIZE`.
- Complex/nested types. The first sqllogictests cover a scalar matrix including integers, unsigned integers, floating point, `DECIMAL`, date/time, `VARCHAR`, `BLOB`, and `NULL`. `LIST`, `STRUCT`, `MAP`, and other nested shapes are still untested.
- Concurrent COPY to the same file. Sink is serialized through a global lock and Finalize is single-threaded in the PoC, but this has not been stress-tested.

### 6.5 Re-serialization, not byte copy

When the source of the `COPY` is an existing DuckDB-format table, we do **not** memcpy existing compressed segments into the destination. We decompress to `DataChunk`s through the scan (required by the `COPY` operator's contract), buffer them, and recompress on write. This is wasteful for DuckDB-to-DuckDB migrations but is the natural path for any `COPY ... TO`, since the source might be a table, a CTE, a `read_parquet` call, a join result, etc. — all of which produce decompressed chunks.

A zero-copy migration tool for DuckDB-format → `.hduck` is a conceivable future addition but is explicitly out of scope for a data-lake file format: Iceberg / Delta / DuckLake always write from insert batches, never from existing files.

### 6.6 Missing features (intentional scope cut for the PoC)

- No predicate / projection pushdown. `TableFunction::projection_pushdown = false`, no filter propagation.
- No multi-file / globbing. Must migrate to `MultiFileReader` for DuckLake.
- No remote / object-storage reads. All I/O goes through `FileSystem`, so `httpfs` *should* work transparently; untested.
- No encryption.
- No delete files / MVCC. Format is strictly write-once, read-only.
- No parallel scan. `MaxThreads() == 1`.

### 6.7 Format stability

The file format is versioned (`HEADLESS_DUCK_FORMAT_VERSION = 2` at the time of writing). The underlying encodings are whatever DuckDB's current storage writes — so every DuckDB storage-format bump affects us too. Before this format can be used in a data lake, we need a formal stability contract: "version K files are always readable by DuckDB ≥ X." This matters because once a `.hduck` lands in an Iceberg table, any future DuckDB must read it forever.

## 7. Is this actually necessary? Headless format vs `.duckdb`-as-data-file

The premise of this PoC is that DuckLake (and eventually Iceberg / Delta) would benefit from a stripped-down DuckDB-native file format. A reasonable reviewer reaction is: *why not just use `.duckdb` files directly as data files in DuckLake?* This section takes that question seriously.

### 7.1 The alternative: use `.duckdb` files as DuckLake data files

**Write side:** DuckLake's writer creates an in-memory database, writes the insert batch as a single table, checkpoints to a `.duckdb` file, detaches, and registers the file in its manifest.

**Read side:** For each data file that a query covers, DuckLake does `ATTACH 'path.duckdb' (READ_ONLY)`, scans the single table, and unions results. The glue is behind `FROM ducklake_table`; users never see the `ATTACH`s.

This works. DuckDB has mature cross-database query support; the per-file plumbing is well-trodden. You'd ship zero new file-format code.

### 7.2 What the alternative costs

1. **Conceptual mismatch.** `.duckdb` is a *database*: catalog, schemas, indexes, sequences, constraints, views, storage version, `db_identifier`. A data-lake data file is a *slab of rows*. DuckLake would have to specify strong conventions ("exactly one table named `data`, no indexes, no views") or handle the full generality of embedded databases.
2. **Per-file `ATTACH` cost.** Each data file carries kilobytes of catalog; each query that touches N files pays N times the cost of catalog-load + `DatabaseManager` registration + `TransactionManager` init. At 10 files, unnoticeable. At 10,000 files, significant. Parquet's per-file footer read is meaningfully cheaper.
3. **Format stability.** DuckDB's full storage format changes — `storage_version` has been bumped several times. Every bump risks breaking `.duckdb` files sitting in data lakes. The subset used in `.hduck` (no WAL, no catalog, no free list — just the column-segment encoding layer) has far fewer moving parts, so a strong forward-compat guarantee is tractable.
4. **Spec-ability for non-DuckDB engines.** If Iceberg or Delta ever adopt this, other engines (Spark, Trino, Flink) need an independent spec. "A subset of DuckDB's database format" is harder to spec than "a DuckDB columnar container."
5. **Schema evolution.** DuckLake handles schema evolution in the catalog. An embedded `.duckdb` catalog in each data file is redundant and fights back when the logical schema changes.

### 7.3 When the headless format is actually justified

The case is strongest under these conditions:

- **Eventual Iceberg / Delta adoption.** Needs a clean, independently-spec-able file format.
- **Long-term forward-compat guarantee on bytes in data lakes.** The subset here can promise it; the full `.duckdb` format probably can't.
- **Many-small-files query shapes.** Where per-file `ATTACH` cost compounds.

If none of those apply — DuckLake-only, internal use, short-lived files — the `.duckdb`-as-data-file approach is a credible MVP and probably sufficient.

### 7.4 What the PoC bought you regardless

Even if the answer is "use `.duckdb` directly," the work here isn't wasted. The technical result is that DuckDB's columnar writer and reader can be driven **without a catalog or `DatabaseInstance`-owned attached database** — with a plain `BlockManager` subclass as the adapter. That capability is useful for read-only network attachments, virtual catalog overlays, and any future scenario where the storage engine needs to run detached from catalog machinery. The design doc is also a map of where the catalog coupling actually lives.

### 7.5 Recommendation for reviewers

Decide the three conditions in §7.3. If any two of them apply to DuckLake's planned trajectory, `.hduck` (or a successor format with this shape) is worth pursuing. If none do, the simpler `.duckdb`-as-data-file path is probably the right MVP. This PoC is cheap insurance against the answer being "yes, we want the headless format" — most of the real risk (can DuckDB's storage run headless at all?) is now retired either way.

## 8. Open questions for reviewers

1. **Stability commitment.** Can the DuckDB storage format be declared forward-compatible for some defined subset (the encodings we use here)? Without that, this format cannot live in a data lake.
2. **Upstream vs out-of-tree.** Should this become a core extension, a separate repo, or stay in-tree for now? The reader is useful standalone (read a `.hduck` anywhere DuckDB runs); the writer less so until DuckLake integrates it.
3. **Decoupling from `AttachedDatabase`.** See §6.1. Appetite for the upstream refactor?
4. **DuckLake integration surface.** What does DuckLake need from a file format object beyond `read(path) → rows` and `write(path, rows) → stats`? The `file_statistics` plumbing isn't wired yet; how does DuckLake consume per-column stats for pruning?
5. **Naming.** `.hduck` as the extension, `headless_duck` as the format name — opinions welcome. Industrial naming may want something more neutral.

## 9. Roadmap

Near-term (days):
- Stress-test and optimize the parallel COPY append path.
- Strip diagnostic `std::cerr` calls.
- Verify multi-row-group files.
- Run full TPC-H `lineitem` size + scan benchmarks against Parquet.

Medium-term (weeks):
- Multi-file support via `MultiFileReader`.
- Projection + predicate pushdown.
- Parallel scan (one thread per row group).
- Expose per-column statistics in `copy_to_get_written_statistics` so DuckLake can manifest-prune.

Larger:
- Prototype DuckLake integration: write `.hduck` on insert, read from `FROM ducklake_table`.
- Upstream `DataTableInfo` decoupling refactor.
- Spec the file format independently of the current DuckDB source layout.
