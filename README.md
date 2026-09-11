# libit - Interval Tree Library

A high-performance C library for storing and querying time-based intervals. Built on top of [qmap](https://github.com/tty-pt/qmap) for efficient sorted storage and iteration.

## What is libit?

libit allows you to:

- **Store intervals**: Record start/stop times for any entity (users, processes, events)
- **Query overlaps**: Find all intervals that intersect a given time range
- **Split intervals**: Decompose overlapping intervals into non-overlapping segments
- **Persist data**: Save intervals to disk and reload them later

## Features

- **High capacity**: Store up to 65,536 intervals per database
- **High overlap support**: Handle up to 4,096 concurrent overlapping entities
- **Fast queries**: Microsecond-level query performance
- **File persistence**: Built on qmap for reliable disk storage
- **Clean API**: Simple C interface with error handling
- **Zero-copy iteration**: Efficient traversal of query results

## Quick Example

```c
#include <ttypt/it.h>

// Create database (NULL = memory only, "data.qmap" = persisted)
unsigned itd = it_init(NULL);

// Record that entity 1 was active from time 1000 to 2000
it_start(itd, 1000, 1);
it_stop(itd, 2000, 1);

// Query: which entities were active between 1200 and 1800?
it_cur_t cur = it_iter(itd, 1200, 1800);
time_t min, max;
unsigned count, who;

while (it_next(&min, &max, &count, &who, &cur)) {
    printf("Entity %u active [%ld, %ld)\n", who, min, max);
}
```

## Installation

```bash
# Clone and build
git clone https://github.com/tty-pt/libit.git
cd libit
make all

# Run tests
./test.sh

# Or run manually
LD_LIBRARY_PATH=./lib ./bin/test
```

## Dependencies

- **qmap** - Sorted key-value store with persistence
- **qsys** - System utilities library
- **libc** - Standard C library (already on your system)

These are automatically built and linked when you run `make`.

## Documentation

| Document | Description |
|----------|-------------|
| [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) | API overview with code examples |
| [CHANGELOG.md](./CHANGELOG.md) | Version history and changes |
| [LIBIT_LIMITATIONS.md](./LIBIT_LIMITATIONS.md) | Known limitations and workarounds |
| [TESTING_SUMMARY.md](./TESTING_SUMMARY.md) | Test coverage and results |

## Recall Kernel Adapter (roadmap — W4)

libit is a time axis for the recall kernel (`rec.h` in libqmap; spec in
libqmap's `docs/RECALL-KERNEL.md`). The planned adapter — **not yet
implemented** — follows the contract (one filler, streams matches, seals,
plain `int` return, additive):

```c
/* Proposed (W4), not implemented. */
int rec_axis_fill_interval(unsigned itd, time_t a, time_t b, rec_set_t *out);
```

Exact `[a,b)` interval membership, entity id widened to `rec_ref_t`. Until
it lands, compose libit with the existing `it_iter`/`it_next` cursor and
push into a `rec_set_t` yourself.

## API Overview

| Function | Description |
|----------|-------------|
| `it_init(fname)` | Create/open database (NULL = memory only) |
| `it_start(itd, time, id)` | Record interval start for entity |
| `it_stop(itd, time, id)` | Record interval stop for entity |
| `it_iter(itd, min, max)` | Create query iterator |
| `it_next(...)` | Get next result from iterator |
| `it_split(itd, min, max)` | Split overlapping intervals |

See [include/ttypt/it.h](./include/ttypt/it.h) for complete API documentation.

## Performance

Build flags: `-O3 -mpopcnt -mavx2 -mfma` (see `Makefile`). The query path
uses contiguous arenas for matches and splits plus an ephemeral
open-addressing entity set (no per-match/per-entity mallocs, no temp
qmap per gap). Record the numbers yourself:

```bash
make bench   # bin/test_extended with µs timing lines
```

Recent medians (µs, same loaded box, `-O0` baseline vs this tree):
query-all 10k 3.67M→1.60M (2.3×), 1k-overlap query 5.4k→326 (16.7×),
3k-overlap query 62k→1.3k (48.5×), 1000 insert/query cycles
180.8M→26.7M (6.8×), splits 343→62 (5.5×), inserts ~2×. Full table in
[CHANGELOG.md](./CHANGELOG.md) (`[Unreleased]`).

## Version

Current: **v1.2.1**

See [CHANGELOG.md](./CHANGELOG.md) for release history.

## License

See repository for details.

## Acknowledgments

- [qmap](https://github.com/tty-pt/qmap) - Underlying storage engine
- Leon - Debugging assistance
