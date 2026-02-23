# libit v1.2.1 Quick Reference

A quick reference guide for developers using libit (Interval Tree Library).

## What's New in v1.2.0

✅ **32x more intervals:** 2,048 → 65,536 per database  
✅ **16x more overlaps:** 256 → 4,096 entities per split  
✅ **Input validation:** Automatic error detection with errno  
✅ **Better docs:** Updated API documentation

See CHANGELOG.md for complete details.

## Installation & Building

```bash
# Clone repository
git clone https://github.com/tty-pt/libit.git
cd libit

# Build library and tests
make all

# Run test suite
./test.sh
```

## Basic Usage

### Initialize Database
```c
#include <ttypt/it.h>

// Memory-only database
unsigned itd = it_init(NULL);

// File-backed database (NOT WORKING - see Known Issues)
unsigned itd = it_init("mydata.qmap");
```

### Record Intervals
```c
// Entity 1 active from time 1000 to 2000
int ret = it_start(itd, 1000, 1);
if (ret == -1) {
    // Validation error (v1.2.0+)
    if (errno == ERANGE) perror("Timestamp out of range");
    if (errno == EINVAL) perror("Invalid entity ID");
}
it_stop(itd, 2000, 1);

// Entity 2 active from time 1500 to 2500 (overlaps entity 1)
it_start(itd, 1500, 2);
it_stop(itd, 2500, 2);

// Entity 3 still active (no stop yet)
it_start(itd, 3000, 3);
```

### Query Intervals
```c
// Find all entities active between time 1200 and 1800
it_cur_t cur = it_iter(itd, 1200, 1800);
time_t min, max;
unsigned count, who;

while (it_next(&min, &max, &count, &who, &cur)) {
    printf("Interval [%ld, %ld): %u entities, entity ID %u\n", 
           min, max, count, who);
    // Note: Call it_next() 'count' times to get all entity IDs in this interval
}
```

### Time Utilities
```c
// Parse ISO-8601 or Unix timestamp
time_t t = sscantime("2024-01-15T10:30:00");
time_t t2 = sscantime("1705318200");

// Format timestamp to string
char buf[DATE_MAX_LEN];
printtime(buf, t);  // "2024-01-15T10:30:00"
```

### Cleanup (Future Use)
```c
// Currently ineffective due to qmap bugs
it_close(itd);
```

## Important Limits

📊 **Increased limits in v1.2.0:**

| Limit | v1.1.0 | v1.2.0 | What It Means |
|-------|--------|--------|---------------|
| **Max intervals** | ~2,048 | **65,536** | Per database instance (TI_MASK) |
| **Max overlapping entities** | 256 | **4,096** | Per time period (SPLITS_WHO_MASK) |
| **Max entity ID** | 4,294,967,294 | 4,294,967,294 | Cannot use UINT32_MAX (validated) |
| **Timestamp range** | Unchecked | **[LONG_MIN/2, LONG_MAX/2]** | Validated (errno=ERANGE) |
| **Min interval duration** | 1 | 1 | Zero-duration intervals not supported |

### Error Handling (v1.2.0+)

**it_start() and it_stop() return values:**
- `0` = Success
- `1` = Duplicate start (it_start) or no open interval (it_stop)
- `-1` = **Validation error** (check errno)

**errno values:**
- `ERANGE` = Timestamp outside valid range [LONG_MIN/2, LONG_MAX/2]
- `EINVAL` = Entity ID is UINT32_MAX (reserved sentinel)

**Example:**
```c
#include <errno.h>

if (it_start(itd, timestamp, entity_id) == -1) {
    if (errno == ERANGE) {
        fprintf(stderr, "Timestamp %ld out of range\n", timestamp);
    } else if (errno == EINVAL) {
        fprintf(stderr, "Entity ID %u is reserved\n", entity_id);
    }
    return -1;
}
```

### Workarounds

**Too many intervals? (v1.2.0: Much higher limit!)**
```c
// v1.2.0: Can now handle up to 65,536 intervals per database
// Only need multiple databases if exceeding this:
unsigned itd1 = it_init(NULL);  // First 65k intervals
unsigned itd2 = it_init(NULL);  // Next 65k intervals (if needed)
```

**Too many overlaps? (v1.2.0: 16x increase!)**
```c
// v1.2.0: Can now handle up to 4,096 overlapping entities
// Only an issue if more than 4,000 entities overlap simultaneously
```

**Need point events?**
```c
// DON'T: it_start(itd, t, id); it_stop(itd, t, id);  // Zero-duration
// DO:
it_start(itd, t, id);
it_stop(itd, t + 1, id);  // Minimum duration of 1
```

## Known Issues

### ✅ File Persistence NOW WORKING (v1.2.1+)
```c
// This NOW works in v1.2.1+:
unsigned itd = it_init("data.qmap");
// ... operations ...
it_close(itd);
// Data is saved and loaded on next it_init("data.qmap")!
```

**Note**: In v1.2.1, removed QM_MIRROR flag (qmap v0.7.0+ no longer requires it)

### ✅ Design Limitations (All Fixed in v1.2.1!)

**Fixed in v1.2.1:**
- ✅ File persistence: Now working!
- ✅ TI_MASK limit: 2,048 → 65,536 intervals
- ✅ SPLITS_WHO_MASK limit: 256 → 4,096 entities
- ✅ Extreme timestamps: Now validated
- ✅ UINT32_MAX entity ID: Now validated

**Still not supported:**
- ❌ Zero-duration intervals (start == stop) - by design

See `LIBIT_LIMITATIONS.md` for detailed explanations.

## Common Patterns

### Pattern 1: Non-Overlapping Sessions
```c
unsigned itd = it_init(NULL);

// User 1 session: 9:00-10:00
it_start(itd, 900, 1);
it_stop(itd, 1000, 1);

// User 2 session: 10:30-11:30
it_start(itd, 1030, 2);
it_stop(itd, 1130, 2);

// Query who was active at 10:15
it_cur_t cur = it_iter(itd, 1015, 1016);
// Result: No one (between sessions)
```

### Pattern 2: Overlapping Activity
```c
unsigned itd = it_init(NULL);

// Server 1: 8:00-18:00
it_start(itd, 800, 1);
it_stop(itd, 1800, 1);

// Server 2: 12:00-20:00
it_start(itd, 1200, 2);
it_stop(itd, 2000, 2);

// Query who was active at 14:00
it_cur_t cur = it_iter(itd, 1400, 1401);
// Result: Both servers 1 and 2
```

### Pattern 3: Still Active (Open Interval)
```c
unsigned itd = it_init(NULL);

// Service started at 10:00, still running
it_start(itd, 1000, 1);
// No it_stop() call = open interval

// Query current activity (time = 1500)
it_cur_t cur = it_iter(itd, 1500, 1501);
// Result: Service 1 still active
```

### Pattern 4: Finding Gaps
```c
// Find periods where NO entities are active
it_cur_t cur = it_iter(itd, start_time, end_time);
time_t last_max = start_time;

while (it_next(&min, &max, &count, &who, &cur)) {
    if (min > last_max) {
        printf("Gap: [%ld, %ld)\n", last_max, min);
    }
    last_max = max;
}

// Check final gap
if (last_max < end_time) {
    printf("Gap: [%ld, %ld)\n", last_max, end_time);
}
```

### Pattern 5: Count Concurrent Entities
```c
it_cur_t cur = it_iter(itd, query_start, query_end);
time_t min, max;
unsigned count, who;

while (it_next(&min, &max, &count, &who, &cur)) {
    printf("Period [%ld, %ld): %u concurrent entities\n", 
           min, max, count);
    
    // To get all entity IDs in this period:
    // - First call got 'who' for first entity
    // - Call it_next() (count - 1) more times to get remaining IDs
}
```

## Error Handling

### Time Parsing Errors
```c
errno = 0;
time_t t = sscantime("invalid");
if (errno != 0) {
    perror("sscantime failed");
    // Invalid input
}
```

### Invalid Operations
```c
// Stop before start (error case)
it_start(itd, 2000, 1);
it_stop(itd, 1000, 1);  // ERROR: stop < start
// Result: Undefined behavior
```

### Multiple Stops (Idempotent)
```c
it_start(itd, 1000, 1);
it_stop(itd, 2000, 1);
it_stop(itd, 2000, 1);  // OK: idempotent
it_stop(itd, 2500, 1);  // Updates to new stop time
```

## Performance Tips

Based on Phase 4 benchmarks:

### Fast Operations
- **Interval insertion**: ~70 µs per interval
- **Query sparse data**: ~12 µs (few matches)
- **Time parsing**: ~1 µs per operation
- **Split computation**: ~87 µs for 100 entities

### Slow Operations
- **Query large datasets**: ~48 ms for 2000 intervals
- **Many overlaps**: Performance degrades with overlap count

### Optimization Strategies

**Minimize Overlaps**
```c
// BAD: All entities start at same time
for (int i = 0; i < 1000; i++) {
    it_start(itd, 0, i);
    it_stop(itd, 1000, i);
}
// Result: 1000 overlaps, limited to 256 by SPLITS_WHO_MASK

// BETTER: Stagger start times
for (int i = 0; i < 1000; i++) {
    it_start(itd, i * 10, i);
    it_stop(itd, i * 10 + 5, i);
}
// Result: Minimal overlaps, better performance
```

**Partition Large Datasets**
```c
// Instead of one database with 10,000 intervals:
unsigned itd = it_init(NULL);  // Hits 2048 limit!

// Use multiple databases:
unsigned itd_2020 = it_init(NULL);  // Year 2020 data
unsigned itd_2021 = it_init(NULL);  // Year 2021 data
unsigned itd_2022 = it_init(NULL);  // Year 2022 data
// Each can hold up to 2048 intervals
```

**Narrow Query Ranges**
```c
// SLOW: Query entire timeline
it_cur_t cur = it_iter(itd, 0, LONG_MAX);

// FAST: Query specific time window
it_cur_t cur = it_iter(itd, 1000, 2000);
```

## Testing

```bash
# Run core tests (Categories 1-6)
LD_LIBRARY_PATH=./lib ./bin/test

# Run extended tests (stress, performance, edge cases)
LD_LIBRARY_PATH=./lib ./bin/test_extended

# Run complete test suite
./test.sh
```

## Documentation

- **TESTING_SUMMARY.md**: Complete testing overview
- **LIBIT_LIMITATIONS.md**: Detailed limitation explanations
- **QMAP_PERSISTENCE_BUGS.md**: Persistence bug investigation
- **CHANGELOG.md**: Version history and changes
- **include/ttypt/it.h**: API reference (Doxygen comments)

## API Reference

### Core Functions

| Function | Description |
|----------|-------------|
| `it_init(fname)` | Initialize database (NULL for memory-only) |
| `it_start(itd, time, who)` | Start interval for entity |
| `it_stop(itd, time, who)` | Stop interval for entity |
| `it_iter(itd, min, max)` | Create query iterator |
| `it_next(...)` | Get next result from iterator |
| `it_close(itd)` | Close database (saves data in v1.2.1+) |

### Time Functions

| Function | Description |
|----------|-------------|
| `sscantime(buf)` | Parse ISO-8601 or Unix timestamp |
| `printtime(buf, t)` | Format timestamp to string |

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `DATE_MAX_LEN` | 30 | Max length for printtime() buffer |

## Compile & Link

```bash
# Compile your program
gcc -o myapp myapp.c -I/path/to/libit/include

# Link against libit and dependencies
gcc -o myapp myapp.o -L/path/to/libit/lib -lit -lqmap -lqsys
```

Or use pkg-config:
```bash
gcc -o myapp myapp.c $(pkg-config --cflags --libs libit)
```

## Getting Help

- **GitHub Issues**: https://github.com/tty-pt/libit/issues
- **Source Code**: `/home/quirinpa/libit/src/libit.c`
- **Test Examples**: `/home/quirinpa/libit/src/test.c`, `test_extended.c`

## Version Info

- **Version**: libit v1.1.0
- **Dependencies**: qmap v0.6.0, qsys
- **Status**: Production-ready for in-memory use
- **Known Issues**: File persistence not working (qmap bugs)
- **License**: BSD-2-Clause

---

*Quick reference for libit v1.1.0 - Generated from comprehensive testing (Phases 1-4)*
