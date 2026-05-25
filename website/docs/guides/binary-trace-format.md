---
sidebar_label: "Binary Trace Format Specification"
---

# Binary Trace Format Specification

## Overview

Chronon's binary trace format (`.ctrace`) provides compact, self-describing trace storage using FlatBuffers for schema and zstd for compression.

## File Structure

```
┌─────────────────────────────────────┐
│ File Header (64 bytes, fixed)       │
├─────────────────────────────────────┤
│ Schema Section (FlatBuffers)        │
├─────────────────────────────────────┤
│ Event Block 0                       │
│   ├── Block Header (28 bytes)       │
│   └── Compressed Event Data         │
├─────────────────────────────────────┤
│ Event Block 1...N                   │
├─────────────────────────────────────┤
│ Footer (FlatBuffers)                │
└─────────────────────────────────────┘
```

## File Header (64 bytes)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | magic | `0x43545243` ("CTRC" little-endian) |
| 4 | 2 | version_major | Format version major (1) |
| 6 | 2 | version_minor | Format version minor (1) |
| 8 | 8 | schema_offset | Byte offset to schema section |
| 16 | 8 | schema_size | Size of schema in bytes |
| 24 | 8 | first_block_offset | Byte offset to first event block |
| 32 | 8 | footer_offset | Byte offset to footer |
| 40 | 8 | footer_size | Size of footer in bytes |
| 48 | 8 | flags | Feature flags (see below) |
| 56 | 8 | reserved | Reserved for future use |

### Flags

| Bit | Name | Description |
|-----|------|-------------|
| 0 | FLAG_COMPRESSED | Event data is zstd compressed |
| 1 | FLAG_HAS_INDEX | Footer contains block index |
| 2 | FLAG_HAS_SCHEMA | Schema section present |

## Schema Section

FlatBuffers-encoded schema containing:

```fbs
table TraceSchema {
    version: uint32;
    formats: [FormatEntry];
    categories: [CategoryEntry];
    units: [UnitEntry];
    simulation_name: string;
    config_hash: uint64;
}

table FormatEntry {
    id: uint32;
    format_string: string;
    file: string;
    line: uint32;
    arg_types: [ArgType];
    is_log: bool;
    log_level: LogLevel;
}

table CategoryEntry {
    name: string;
    description: string;
    mask: uint64;
}

table UnitEntry {
    id: uint16;
    name: string;
    type_name: string;
}
```

## Event Block

Each block contains multiple events with metadata for seeking and compression.

### Block Header Dual Representation

The block header exists in two forms:

1. **Inline Binary (28 bytes)**: Written before each compressed block for streaming parsing
2. **FlatBuffers `BlockHeader`**: Stored in the file footer for indexed random access

Both representations contain the same information but serve different purposes:
- Inline binary enables sequential streaming without parsing the footer
- FlatBuffers footer enables efficient cycle-based seeking

### Inline Block Header (28 bytes)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | event_count | Number of events in block |
| 4 | 8 | min_cycle | Minimum cycle in block |
| 12 | 8 | max_cycle | Maximum cycle in block |
| 20 | 4 | uncompressed_size | Size before compression |
| 24 | 4 | compressed_size | Size after compression |

### Event Data

After the block header, compressed event data follows. Each event is stored as:

```
[args_size:2][StructuredRecord:24][args:N]
```

Where `args_size` is a 2-byte prefix (added in v1.1) indicating the size of the argument data that follows the record.

**Important:** The in-memory runtime representation (`StructuredRecord` in Types.hpp) differs slightly from the FlatBuffers schema representation (`EventRecord` in TraceSchema.fbs). Both are 24 bytes but use different field names. The binary format uses `StructuredRecord` layout.

#### StructuredRecord (24 bytes)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | cycle | Simulation cycle |
| 8 | 4 | format_id | Index into schema formats |
| 12 | 4 | category | Category bitmask (truncated to 32 bits) |
| 16 | 2 | source_id | Unit ID |
| 18 | 1 | arg_count | Number of arguments |
| 19 | 1 | padding | Alignment padding |

#### Argument Types

| Value | Type | Size |
|-------|------|------|
| 1 | Int8 | 1 |
| 2 | Int16 | 2 |
| 3 | Int32 | 4 |
| 4 | Int64 | 8 |
| 5 | UInt8 | 1 |
| 6 | UInt16 | 2 |
| 7 | UInt32 | 4 |
| 8 | UInt64 | 8 |
| 9 | Float | 4 |
| 10 | Double | 8 |
| 11 | Pointer | 8 |
| 12 | String | Variable (null-terminated) |
| 13 | Bool | 1 |

## Footer

FlatBuffers-encoded index for random access:

```fbs
table FileFooter {
    blocks: [BlockHeader];
    total_events: uint64;
    min_cycle: uint64;
    max_cycle: uint64;
    created_timestamp: uint64;
}

table BlockHeader {
    block_index: uint32;
    event_count: uint32;
    min_cycle: uint64;
    max_cycle: uint64;
    uncompressed_size: uint32;
    compressed_size: uint32;
    data_offset: uint64;
}
```

## Version History

| Version | Changes |
|---------|---------|
| 1.0 | Initial format |
| 1.1 | Added per-event `args_size` prefix for robust parsing |

## Reading Traces

### Using trace_reader CLI

The `trace_reader` tool provides several commands for analyzing trace files:

```bash
# Show trace file information and statistics
trace_reader info events.ctrace

# Show verbose info with all registered formats and units
trace_reader info events.ctrace --verbose

# Dump all events as text
trace_reader dump events.ctrace

# Filter events by cycle range
trace_reader filter events.ctrace --cycles 1000-2000

# Filter by unit name
trace_reader filter events.ctrace --unit "cpu.Fetch"

# Limit output to first N events
trace_reader dump events.ctrace --limit 100

# Convert to text file
trace_reader convert events.ctrace -o output.log

# Combine filters
trace_reader filter events.ctrace --cycles 5000-10000 --unit "Decode" --limit 50 -o filtered.log
```

### Using BinaryTraceReader API

For programmatic access:

```cpp
#include "chronon/Chronon.hpp"

BinaryTraceReader reader;
if (reader.open("events.ctrace")) {
    while (auto event = reader.readEvent()) {
        std::cout << "[" << event->cycle << "] "
                  << reader.reconstructMessage(*event) << "\n";
    }
}
```

> **Note**: The default output file is `events.ctrace` (not `trace.ctrace`). Both trace events
> and log events can appear in the binary file if their channel format is set to `binary` or `both`.

## Compression

- **Algorithm**: zstd (level 3 default)
- **Block size**: 64KB before compression (configurable)
- **Typical ratio**: ~20% (5x compression)

Compression is optional and indicated by `FLAG_COMPRESSED`.
