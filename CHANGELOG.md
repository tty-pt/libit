# Changelog

All notable changes to libit will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.1] - 2026-02-23

### Fixed - File Persistence

**File Persistence Now Works** ✅
- Removed QM_MIRROR flag from `it_init()` (src/libit.c:166)
- **Root cause:** qmap v0.7.0+ no longer requires QM_MIRROR for file persistence
- **Solution:** Changed flags from `QM_MIRROR` to `0`
- **Why it works:** libit doesn't need bidirectional lookups (qmap_assoc), so QM_MIRROR was unnecessary

**Test Results:**
- All 5 Category 7 persistence tests now passing:
  - test_persist_save_load ✅
  - test_persist_multiple_intervals ✅
  - test_persist_empty_database ✅
  - test_persist_append ✅
  - test_persist_large_dataset ✅

**Total Test Count:** 74 tests (59 core + 15 extended)

### Documentation Updated
- LIBIT_LIMITATIONS.md: Added section 5 for persistence fix
- QMAP_PERSISTENCE_BUGS.md: Marked as resolved

---

## [1.2.0] - 2026-02-23

### Fixed - Design Limitations Addressed

**TI_MASK Limit (Fixed)**
- Increased from `0x7FF` to `0xFFFF` (src/libit.c:28)
- **Old limit:** ~2,048 intervals per database
- **New limit:** 65,536 intervals per database  
- **Increase:** 32x capacity
- Test coverage: 10k, 15k, and 20k interval tests passing

**SPLITS_WHO_MASK Limit (Fixed)**
- Increased from `0xFF` to `0xFFF` (src/libit.c:27)
- **Old limit:** 256 entities per split interval
- **New limit:** 4,096 entities per split interval
- **Increase:** 16x capacity
- Test coverage: 1k and 3k overlapping entity tests passing

**Extreme Timestamps (Fixed)**
- Added input validation to `it_start()` and `it_stop()`
- **Valid range:** `[LONG_MIN/2, LONG_MAX/2]`
- **Behavior:** Returns -1 with `errno = ERANGE` for out-of-range timestamps
- **Rationale:** Prevents conflicts with internal sentinel values (mtinf, tinf)
- Test coverage: Category 8 validation tests

**UINT32_MAX Entity ID (Fixed)**
- Added input validation to reject UINT32_MAX
- **Behavior:** Returns -1 with `errno = EINVAL` for UINT32_MAX entity ID
- **Rationale:** Prevents conflicts with IDM_MISS sentinel (0xFFFFFFFF)
- **Valid range:** 0 to 4,294,967,294 (0x00000000 to 0xFFFFFFFE)
- Test coverage: Category 8 validation tests

### Changed - API Behavior

**it_start() and it_stop() Return Values**
- **Previous:** 0=success, 1=duplicate/no-interval
- **New:** 0=success, 1=duplicate/no-interval, -1=validation error
- **Error reporting:** Check `errno` when return value is -1
  - `ERANGE`: Timestamp outside valid range
  - `EINVAL`: Entity ID is UINT32_MAX

### Added

**Category 8: Input Validation Tests** (4 new tests)
- test_validation_extreme_timestamp_start: Validates timestamp range in it_start()
- test_validation_extreme_timestamp_stop: Validates timestamp range in it_stop()
- test_validation_uint32_max_start: Validates entity ID in it_start()
- test_validation_uint32_max_stop: Validates entity ID in it_stop()

**Extended Test Suite Enhancements** (3 new boundary tests)
- Test 13: 15,000 intervals (approaching 65k limit)
- Test 14: 3,000 overlapping entities (approaching 4k limit)  
- Test 15: 20,000 intervals (30% of TI_MASK limit)

**Updated test limits:**
- Test 1: 2,000 → 10,000 intervals
- Test 2: 250 → 1,000 overlapping entities

**Total test count:** 69 tests (54 core + 15 extended)

### Updated Documentation

- **LIBIT_LIMITATIONS.md**: Marked 4 limitations as FIXED, reorganized by status
- **QUICK_REFERENCE.md**: Updated limits table and error handling examples
- **TESTING_SUMMARY.md**: Added v1.2.0 fixes section
- **it.h**: Updated API documentation for it_start() and it_stop() with new return codes

### Known Issues

**Zero-Duration Intervals (Not Fixed - By Design)**
- Intervals where start==stop remain unsupported
- Query mechanism uses exclusive upper bounds, making point-in-time queries inconsistent
- Workaround: Use minimum duration of 1 time unit

**Persistence Tests (Still Failing)**
- Re-tested with qmap b1bc322 (includes df5a7ac file loading fix)
- Result: Segmentation fault - bugs still present
- Category 7 tests remain disabled pending upstream qmap fixes
- See QMAP_PERSISTENCE_BUGS.md for updated test results

### Performance

Performance remains excellent with increased limits:
- 10k intervals: 350.86 µs/interval insertion
- 1k overlapping entities: 19.8 ms total creation time
- 20k intervals: 589.65 µs/interval insertion
- Query performance: <1ms for most queries

### Compatibility

- **qmap version:** b1bc322+ (tested with b1bc322)
- **Breaking changes:** None - backward compatible API
- **New behavior:** Validation errors return -1 (previously would silently fail or corrupt data)

## [1.1.0] - 2026-02-23

### Changed
- Align project structure with qmap v0.6.0 patterns
- Update .gitignore with build artifact and test file patterns (/bin, /*.db, /*.qmap, /man)
- Add Doxygen support for automatic man page generation
- Enhance API documentation in it.h with comprehensive Doxygen comments
- Update dependencies in it.pc (libqmap, libqsys instead of libqdb, libdb)
- Establish CHANGELOG for version tracking

### Fixed
- Fixed missing qmap_fin() calls in ti_intersect() and split_create() causing memory leaks (Phase 2)
- Fixed integer underflow bug in splits_create() when matches_l=0 causing infinite loop and corruption (Phase 2)
- Added proper empty result handling in splits_get() for query ranges with no matches (Phase 2)
- Fixed errno not being reset in sscantime() before strtoull() call, causing false positives (Phase 3 - src/libit.c:87)
- Fixed printtime() missing return statements after setting "-inf"/"inf", preventing crashes on extreme timestamps (Phase 3 - src/libit.c:106, 111)

### Added
- Comprehensive test suite (Phases 1, 2, 3 & 4):
  - Category 1: Basic initialization tests (3 tests)
  - Category 2: Basic start/stop operation tests (10 tests)
  - Category 3: Multiple entities tests (15 tests)
  - Category 4: Intersection query tests (8 tests)
  - Category 5: Split computation tests (8 tests)
  - Category 6: Time utilities tests (6 tests - sscantime, printtime)
  - Category 7: Persistence tests (5 tests - DISABLED due to qmap bugs)
  - **Category 8: Extended tests** (12 tests - stress, performance, edge cases):
    - Test 1: Large dataset stress test (2000 intervals)
    - Test 2: Many overlapping intervals (250 entities)
    - Test 3: Sequential insertion performance benchmark
    - Test 4: Query performance on sparse data
    - Test 5: Extreme timestamp values (INT64_MAX, negative timestamps)
    - Test 6: Split computation performance (100 overlapping entities)
    - Test 7: Repeated operations (memory leak detection)
    - Test 8: Boundary query performance
    - Test 9: Entity ID edge cases (ID=0, ID=UINT32_MAX)
    - Test 10: Time utility functions stress test (10,000 parse/format operations)
    - Test 11: Interleaved operations stress test (500 entities)
    - Test 12: Zero-duration intervals (start == stop)
- Total: 62 automated tests covering all functionality plus performance benchmarks
- test_extended binary with performance metrics (microsecond timing, throughput reporting)
- Integration test script (test.sh) with regression testing via expects.txt
- Test output formatting with ✅/❌ indicators for easy visual verification
- Added it_close() function for future persistence support (currently no-op for in-memory databases)
- Documentation of qmap persistence bugs (see QMAP_PERSISTENCE_BUGS.md)

### Known Limitations
**Discovered during Phase 4 extended testing:**
- **TI_MASK limit**: Maximum ~2048 intervals per database (TI_MASK=0x7FF in src/libit.c:28)
- **SPLITS_WHO_MASK limit**: Maximum 256 entities per split interval (SPLITS_WHO_MASK=0xFF in src/libit.c:27)
- **Extreme timestamps**: Values near INT64_MAX may overflow on some platforms
- **UINT32_MAX entity ID**: Conflicts with IDM_MISS sentinel value (0xFFFFFFFF)
- **Zero-duration intervals**: Intervals where start==stop are not supported (may not be queryable)

### Notes
- Code is fully compatible with qmap v0.6.0 (updated in v1.0.0)
- Benefits from qmap v0.6.0 improvements:
  - Improved pointer stability (allocation reuse)
  - Automatic file loading for persistence
  - Enhanced qmap documentation
- **File persistence currently NOT WORKING** due to critical bugs in qmap v0.6.0:
  - Bug #1: Multiple databases per file with QM_MIRROR fails to persist data
  - Bug #2: Process exit crash with QM_MIRROR and custom types ("free(): invalid pointer")
  - See QMAP_PERSISTENCE_BUGS.md for detailed investigation and reproduction steps
  - Persistence tests (Category 7) are implemented but disabled until qmap is fixed
  - it_close() function added for future persistence support but currently ineffective

## [1.0.0] - 2025-10-26

### Added
- Initial stable release
- Interval tree implementation using qmap v0.5.0+
- File-based persistence support via qmap
- Multiple database support per file
- Sorted iteration via QM_SORTED
- BTREE secondary indexes for efficient queries (by max time, by ID)
- ISO-8601 date string parsing and formatting utilities

### Changed
- Migrated from libqdb to libqmap
- Updated from `unsigned` to `uint32_t` types throughout
- Adopted qmap's persistent storage model
- Reorganized headers to ttypt/ namespace
