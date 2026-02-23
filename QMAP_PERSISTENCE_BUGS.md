# qmap Persistence Bugs - Investigation Summary

## Date
February 23, 2026

## Context
While implementing Phase 3 of libit v1.1.0 testing (persistence tests), we discovered critical bugs in qmap v0.6.0's file persistence functionality.

## Bugs Discovered

### Bug 1: Multiple Databases Per File with QM_MIRROR Fails
**Severity**: Critical  
**Status**: Confirmed

**Description**:  
When multiple qmap databases are created on the same file using `QM_MIRROR`, data persistence fails completely. Data is not loaded back after closing and reopening.

**Reproduction**:
```c
// Create 3 databases on same file
hd1 = qmap_open("/tmp/multi.db", "db1", QM_U32, QM_U32, 0xFF, QM_MIRROR);
hd2 = qmap_open("/tmp/multi.db", "db2", QM_U32, QM_U32, 0xFF, QM_MIRROR);
hd3 = qmap_open("/tmp/multi.db", "db3", QM_U32, QM_U32, 0xFF, QM_MIRROR);

// Insert data
qmap_put(hd1, &key, &val);

// Save and close
qmap_save();
qmap_close(hd1);
qmap_close(hd2);
qmap_close(hd3);

// Reopen
hd1 = qmap_open("/tmp/multi.db", "db1", QM_U32, QM_U32, 0xFF, QM_MIRROR);

// Query - DATA NOT FOUND!
result = qmap_get(hd1, &key);  // Returns NULL
```

**Expected**: Data should persist and be loaded back.  
**Actual**: `qmap_get()` returns NULL - data not found.

**Workaround**: Use separate files for each database instead of multiple databases per file.

**Test Files**:
- `/tmp/multi_db_test.c` - Reproduces with custom types
- `/tmp/multi_db_test2.c` - Reproduces with built-in types (QM_U32)

---

### Bug 2: Process Exit Crash with QM_MIRROR and Custom Types
**Severity**: Critical  
**Status**: Confirmed

**Description**:  
When using `QM_MIRROR` with custom registered types and file-backed databases, the program crashes during exit cleanup with "free(): invalid pointer" error.

**Reproduction**:
```c
// Register custom type
uint32_t qm_struct = qmap_reg(sizeof(struct my_struct));

// Create file-backed database with QM_MIRROR
hd = qmap_open("/tmp/test.db", "db", qm_struct, qm_struct, 0xFF, QM_MIRROR);

// Insert data and save
qmap_put(hd, &key_struct, &val_struct);
qmap_save();

// DON'T close explicitly - let process exit
// CRASH: free(): invalid pointer
```

**Expected**: Clean exit with automatic save via destructor.  
**Actual**: Segfault/abort with memory corruption error during qmap's destructor.

**Workaround**: Explicitly close all file-backed databases before program exit, or avoid QM_MIRROR with custom types.

---

## Impact on libit

### Original Design
libit v1.1.0 was designed to use 3 qmap databases per interval tree:
- `ti`: Primary database (interval -> interval)
- `max`: Secondary index sorted by max timestamp
- `id`: Secondary index sorted by entity ID

All three would share the same file for persistence.

### Workaround Attempted
Changed to use separate files:
- `<fname>-ti` for primary database
- `<fname>-max` for max index
- `<fname>-id` for id index

**Result**: Still crashes due to Bug #2.

### Current Solution
**Persistence disabled** in libit v1.1.0:
1. Added `it_close()` function (for future use)
2. Implemented persistence test infrastructure (Category 7 tests)
3. **Disabled** Category 7 tests with note: "SKIPPED: Persistence tests disabled due to qmap bug"
4. File persistence marked as **NOT WORKING** until qmap bugs are fixed

---

## Files Modified (libit)

### Core Changes
-  `/home/quirinpa/libit/src/libit.c`:
  - Added `it_close()` function (lines 619-630)
  - Fixed `sscantime()` errno bug (line 87)
  - Fixed `printtime()` missing return statements (lines 106, 111)
  - Modified `tidbs_init()` to use QM_MIRROR flag (line 165)

- `/home/quirinpa/libit/include/ttypt/it.h`:
  - Added `it_close()` documentation (lines 88-102)

### Test Infrastructure
- `/home/quirinpa/libit/src/test.c`:
  - Added Category 6 tests (time utilities) - 6 tests
  - Added Category 7 tests (persistence) - 5 tests (DISABLED)
  - Updated test runner to skip persistence tests

---

## Recommendations

### For qmap Maintainers
1. **Fix Bug #1**: Investigate `qmap_load_file()` and `_qmap_load()` to understand why multiple databases per file fail to load
2. **Fix Bug #2**: Investigate memory management in qmap destructor when QM_MIRROR is used with custom types
3. **Add Tests**: qmap's `test_extended.c` only tests built-in types (QM_U32, QM_STR); add tests with custom registered types

### For libit
1. **Short-term**: Keep persistence disabled until qmap bugs are fixed
2. **Medium-term**: Consider alternative approaches:
   - Use built-in types only (requires redesigning data structures)
   - Implement custom persistence layer (bypassing qmap's)
   - Switch to different storage backend (e.g., SQLite, LMDB)
3. **Long-term**: Contribute fixes to qmap or fork for libit-specific needs

---

## Test Evidence

All test programs are in `/tmp/`:
- `qmap_test.c` - Single DB, no QM_MIRROR: ❌ FAILS
- `qmap_test2.c` - Single DB, with QM_MIRROR: ✅ WORKS
- `multi_db_test.c` - Multi DB, custom types: ❌ FAILS (Bug #1)
- `multi_db_test2.c` - Multi DB, built-in types: ❌ FAILS (Bug #1)
- `single_db_test.c` - Single DB per file, built-in types: ✅ WORKS
- `debug_persist*.c` - libit persistence tests: ❌ FAIL (both bugs)
- `append_test.c` - Demonstrates data corruption on reopen

---

## Conclusion

qmap v0.6.0 has fundamental issues with:
1. Multiple databases per file when using QM_MIRROR
2. Custom types with QM_MIRROR causing memory corruption

These bugs block file persistence in libit until resolved. Phase 3 partially completed:
- ✅ Category 6 (Time Utilities): 6 tests passing
- ⏸️ Category 7 (Persistence): 5 tests implemented but disabled

**Total Phase 3 contribution**: 50 tests (44 from Phases 1-2 + 6 from Category 6)
