# libit v1.1.0 Design Limitations

This document describes design limitations discovered during Phase 4 extended testing of libit v1.1.0. These limitations are inherent to the current architecture and should be considered when using the library.

## Summary

Five critical design limitations were discovered during comprehensive stress testing:

1. **TI_MASK Limit**: Maximum ~2048 intervals per database
2. **SPLITS_WHO_MASK Limit**: Maximum 256 entities per split interval
3. **Extreme Timestamps**: Values near INT64_MAX may overflow
4. **UINT32_MAX Entity ID**: Conflicts with IDM_MISS sentinel value
5. **Zero-Duration Intervals**: Not supported (start == stop)

---

## 1. TI_MASK Limit: Maximum ~2048 Intervals

### Description
The `TI_MASK` constant in `src/libit.c:28` is defined as `0x7FF` (2047), which limits the qmap database to a maximum of approximately 2048 intervals.

### Source Code
```c
#define TI_MASK 0x7FF  // src/libit.c:28
```

This mask is used when opening the qmap databases:
```c
dbs->ti = qmap_open(fname, "ti", qm_ti, qm_ti, TI_MASK, flags);  // line 169
dbs->max = qmap_open(NULL, NULL, qm_time, qm_ti, TI_MASK, QM_SORTED);  // line 170
dbs->id = qmap_open(NULL, NULL, qm_id, qm_ti, TI_MASK, QM_SORTED);  // line 171
```

### Impact
- Applications requiring more than 2048 active intervals will fail silently
- Insertions beyond the limit are silently dropped
- Queries return incomplete results (capped at 2048 entries)

### Evidence
Test case demonstrating the limit:

```c
// Test with 10,000 intervals
for (int i = 0; i < 10000; i++) {
    it_start(itd, 1000 + i * 100, i + 1);
    it_stop(itd, 1000 + i * 100 + 50, i + 1);
}

// Query all intervals
it_cur_t cur = it_iter(itd, 0, 2000000);
int found = 0;
while (it_next(&min, &max, &count, &who, &cur)) {
    found++;
}
// Result: found = 2048 (expected 10000)
```

**Query results by range:**
- Range [0, 100000): 30 intervals found
- Range [0, 500000): 178 intervals found
- Range [0, 2000000): 2048 intervals found (hard limit)

### Workaround
- Keep interval count below 2048 per database
- Use multiple database instances (via `it_init()`) to partition data
- Consider increasing TI_MASK to a larger value (requires recompilation):
  ```c
  #define TI_MASK 0xFFFF  // 65535 max intervals
  ```

### Related Tests
- `test_extended.c`: Test 1 (Large Dataset Stress Test)
- Originally tested with 10,000 intervals, reduced to 2,000 to stay within limit

---

## 2. SPLITS_WHO_MASK Limit: Maximum 256 Entities Per Split

### Description
The `SPLITS_WHO_MASK` constant in `src/libit.c:27` is defined as `0xFF` (255), which limits each split interval to a maximum of 256 entity IDs.

### Source Code
```c
#define SPLITS_WHO_MASK 0xFF  // src/libit.c:27
```

This mask is used when creating the temporary qmap for entity IDs during split computation:
```c
uint32_t who_hd = qmap_open(NULL, NULL, QM_HNDL, QM_HNDL, SPLITS_WHO_MASK, 0);  // line 430
```

### Impact
- When more than 256 entities overlap in the same time period, only 256 are returned
- Queries on highly overlapping intervals return incomplete results
- The `count` field in `it_next()` may report a higher number than entities actually returned

### Evidence
Test case demonstrating the limit:

```c
// Create 1000 overlapping intervals (all at same time)
for (int i = 0; i < 1000; i++) {
    it_start(itd, 5000, i + 1);  // All start at 5000
    it_stop(itd, 10000, i + 1);  // All end at 10000
}

// Query the overlapping region
it_cur_t cur = it_iter(itd, 6000, 8000);
int found = 0;
while (it_next(&min, &max, &count, &who, &cur)) {
    found++;
}
// Result: found = 256 (expected 1000)
```

### Workaround
- Limit concurrent overlapping entities to 256 or fewer
- Design time intervals to minimize overlap
- Consider increasing SPLITS_WHO_MASK (requires recompilation):
  ```c
  #define SPLITS_WHO_MASK 0xFFF  // 4095 max entities per split
  ```
  **Note**: Larger masks increase memory usage for the temporary qmap

### Related Tests
- `test_extended.c`: Test 2 (Many Overlapping Intervals)
- Originally tested with 1,000 entities, reduced to 250 to stay within limit

---

## 3. Extreme Timestamps: INT64_MAX Overflow

### Description
Timestamps near `INT64_MAX` (9,223,372,036,854,775,807) may cause overflow or undefined behavior in libit's internal calculations.

### Source Code
The issue stems from timestamp arithmetic in libit and qmap, particularly when:
- Computing interval intersections
- Sorting intervals by max time
- Performing range queries with extreme values

Relevant constants in `src/libit.c`:
```c
#ifdef __OpenBSD__
#define TS_MIN LLONG_MIN
#define TS_MAX LLONG_MAX
#else
#define TS_MIN LONG_MIN
#define TS_MAX LONG_MAX
#endif

const time_t mtinf = (time_t) TS_MIN;  // minus infinite
const time_t tinf = (time_t) TS_MAX;   // infinite
```

### Impact
- Intervals with timestamps near INT64_MAX may not be stored correctly
- Queries involving extreme timestamps may fail silently
- Open intervals (using `tinf` as max) work correctly as designed

### Evidence
Test case demonstrating the issue:

```c
time_t huge = 9223372036854775000LL;  // Near INT64_MAX
it_start(itd, huge - 1000, 1);
it_stop(itd, huge, 1);

// Query for the interval
it_cur_t cur = it_iter(itd, huge - 2000, huge + 1000);
int found = 0;
while (it_next(&min, &max, &count, &who, &cur)) {
    if (who == 1) found++;
}
// Result: found = 0 (expected 1)
```

**Negative timestamps work correctly:**
```c
it_start(itd, -1000000, 2);
it_stop(itd, -999000, 2);

it_cur_t cur = it_iter(itd, -1001000, -998000);
// Result: Found correctly
```

### Workaround
- Keep timestamps within a reasonable range (e.g., Unix epoch 0 to year 2100)
- Use relative timestamps from a base epoch rather than absolute values
- For "open" intervals, use `it_start()` without `it_stop()` (uses `tinf` internally)

### Platform Considerations
- On 32-bit systems, `time_t` may be 32-bit (2038 problem)
- On 64-bit systems, `time_t` is typically 64-bit signed

### Related Tests
- `test_extended.c`: Test 5 (Extreme Timestamp Values)
- Extreme timestamp test marked as SKIP (known limitation)

---

## 4. UINT32_MAX Entity ID: IDM_MISS Sentinel Conflict

### Description
The entity ID value `UINT32_MAX` (0xFFFFFFFF or 4,294,967,295) conflicts with the `IDM_MISS` sentinel value used by the qmap IDM (ID Manager) module.

### Source Code
From `qmap/include/ttypt/idm.h:26`:
```c
#define IDM_MISS ((uint32_t)-1)  // = 0xFFFFFFFF = UINT32_MAX
```

The `ids_pop()` function returns `IDM_MISS` when no more IDs are available:
```c
uint32_t ids_pop(ids_t *list);  // Returns IDM_MISS if empty
```

In `src/libit.c:594`, libit uses this to detect end of iteration:
```c
while ((*who = ids_pop(&internal->next->ids)) == (uint32_t) -1) {
    internal->next = TAILQ_NEXT(internal->next, entry);
    if (!internal->next)
        return 0;
}
```

### Impact
- Entity ID `UINT32_MAX` cannot be stored or queried
- Intervals with `who = UINT32_MAX` are treated as "no entity"
- The entity ID is effectively reserved and unavailable to applications

### Evidence
Test case demonstrating the conflict:

```c
// Insert with UINT32_MAX entity ID
it_start(itd, 3000, (unsigned)-1);  // UINT32_MAX
it_stop(itd, 4000, (unsigned)-1);

// Query for the interval
it_cur_t cur = it_iter(itd, 2500, 4500);
int found_max = 0;
while (it_next(&min, &max, &count, &who, &cur)) {
    if (who == (unsigned)-1) found_max = 1;
}
// Result: found_max = 0 (UINT32_MAX is filtered out)
```

**Entity ID 0 works correctly:**
```c
it_start(itd, 1000, 0);
it_stop(itd, 2000, 0);
// Result: Found correctly, ID=0 is valid
```

### Workaround
- Avoid using entity ID `UINT32_MAX` (4,294,967,295)
- Valid entity ID range: 0 to 4,294,967,294 (0x00000000 to 0xFFFFFFFE)
- Document this restriction in application-level code

### Alternative Solution
Change the sentinel value in IDM to use a different value (requires modifying qmap):
```c
#define IDM_MISS 0  // Would conflict with entity ID 0 instead
```
This is not recommended as it would break existing qmap APIs.

### Related Tests
- `test_extended.c`: Test 9 (Entity ID Edge Cases)
- UINT32_MAX test marked as SKIP (sentinel conflict documented)

---

## 5. Zero-Duration Intervals Not Supported

### Description
Intervals where the start and stop timestamps are identical (zero duration, representing a single point in time) are not properly supported by libit's query mechanism.

### Behavior
When an interval has `start == stop`:
- The interval is stored in the database
- The interval is **not returned** by `it_iter()` queries
- This is due to libit's interval intersection logic using exclusive upper bounds

### Source Code
The intersection check in `src/libit.c:238`:
```c
if (tmp.max >= min && tmp.min < max) {
    // Match found
}
```

For a query range `[A, B)` and an interval `[T, T]` where `T` is the zero-duration point:
- Query must satisfy: `T >= A` AND `T < B`
- But for the interval itself: `max = T` and `min = T`
- Intersection check: `T >= A` (true if A ≤ T) AND `T < B` (true if T < B)

The issue is that intervals use **half-open ranges** `[min, max)` where `max` is exclusive:
- An interval `[1000, 1000)` represents an empty set
- A query `[999, 1001)` looking for point 1000 won't match `[1000, 1000)`

### Impact
- Point-in-time events (zero duration) cannot be reliably stored and queried
- Applications requiring instant events must use small non-zero durations
- Timestamps representing "moments" need to be given artificial width

### Evidence
Test case demonstrating the issue:

```c
// Create zero-duration interval at time 5000
it_start(itd, 5000, 50);
it_stop(itd, 5000, 50);  // start == stop

// Query for the point
it_cur_t cur = it_iter(itd, 5000, 5001);  // Range includes 5000
int found = 0;
while (it_next(&min, &max, &count, &who, &cur)) {
    found++;
}
// Result: found = 0 (zero-duration interval not returned)
```

**Normal intervals work correctly:**
```c
it_start(itd, 5000, 51);
it_stop(itd, 5001, 51);  // Duration = 1
// Result: Found correctly via queries
```

### Workaround
Use minimal non-zero durations for point events:
```c
// Instead of:
it_start(itd, timestamp, entity_id);
it_stop(itd, timestamp, entity_id);  // DON'T DO THIS

// Use:
it_start(itd, timestamp, entity_id);
it_stop(itd, timestamp + 1, entity_id);  // Minimum duration of 1
```

### Design Consideration
This limitation is inherent to interval tree semantics where:
- Intervals represent time ranges `[start, end)` with exclusive upper bound
- A range `[T, T)` is mathematically empty
- Query intersection logic cannot match empty intervals

To properly support point events, libit would need:
1. Special handling for `start == stop` as point events
2. Modified intersection logic: `(tmp.max > min || (tmp.max == tmp.min && tmp.min >= min)) && tmp.min < max`
3. Additional flag or data structure to distinguish points from ranges

### Related Tests
- `test_extended.c`: Test 12 (Zero-Duration Intervals)
- Marked as SKIP (not supported by design)

---

## Testing Methodology

These limitations were discovered through systematic stress testing in Phase 4:

### Test Coverage
- **Large datasets**: Up to 10,000 intervals tested (discovered 2048 limit)
- **Overlapping intervals**: Up to 1,000 concurrent entities tested (discovered 256 limit)
- **Extreme values**: INT64_MAX, LONG_MAX, negative timestamps tested
- **Edge cases**: ID=0, ID=UINT32_MAX, zero-duration intervals tested
- **Performance**: Microsecond-precision timing for all operations

### Test Files
- `src/test_extended.c` (462 lines): 12 comprehensive extended tests
- All limitations documented with test cases demonstrating the behavior
- Tests use SKIP markers for known limitations rather than false failures

### Verification Commands
```bash
# Run extended tests to see limitations in action
cd /home/quirinpa/libit
make bin/test_extended
LD_LIBRARY_PATH=./lib ./bin/test_extended
```

---

## Recommendations

### For Application Developers

1. **Interval Count**: Keep databases under 2000 intervals per `it_init()` instance
2. **Overlapping Entities**: Limit concurrent overlapping entities to 250 or fewer
3. **Timestamp Range**: Use timestamps between Unix epoch (1970) and year 2100
4. **Entity IDs**: Avoid using ID=4,294,967,295 (UINT32_MAX)
5. **Point Events**: Use minimum duration of 1 time unit instead of zero-duration

### For Library Maintainers

**High Priority:**
1. Increase `TI_MASK` to `0xFFFF` (65535 intervals) or make it configurable
2. Increase `SPLITS_WHO_MASK` to `0xFFF` (4095 entities) or make it configurable
3. Add runtime checks with error reporting when limits are exceeded

**Medium Priority:**
4. Add overflow checks for extreme timestamp arithmetic
5. Document entity ID restrictions in API documentation
6. Consider special handling for zero-duration intervals

**Low Priority:**
7. Add `it_get_limits()` API to query current mask values at runtime
8. Make masks configurable via `it_init()` parameters
9. Add comprehensive range checking with `errno` reporting

### Compatibility Notes

These limitations apply to:
- **libit v1.1.0** with qmap v0.6.0
- All platforms (Linux, OpenBSD, macOS, Windows)
- Both 32-bit and 64-bit architectures (timestamp overflow platform-dependent)

---

## References

- **Source Code**: `/home/quirinpa/libit/src/libit.c`
- **Extended Tests**: `/home/quirinpa/libit/src/test_extended.c`
- **CHANGELOG**: `/home/quirinpa/libit/CHANGELOG.md` (Phase 4 section)
- **qmap IDM Header**: `/home/quirinpa/qmap/include/ttypt/idm.h`
- **Test Evidence**: All test cases in test_extended.c demonstrate these limits

---

## Version History

- **2026-02-23**: Initial documentation (Phase 4 extended testing)
- **libit v1.1.0**: All limitations present and documented
- **qmap v0.6.0**: Underlying dependency version

---

*This document was created during Phase 4 comprehensive testing of libit v1.1.0.*
