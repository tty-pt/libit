# Changelog

All notable changes to libit will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-02-23

### Changed
- Align project structure with qmap v0.6.0 patterns
- Update .gitignore with build artifact and test file patterns (/bin, /*.db, /*.qmap, /man)
- Add Doxygen support for automatic man page generation
- Enhance API documentation in it.h with comprehensive Doxygen comments
- Update dependencies in it.pc (libqmap, libqsys instead of libqdb, libdb)
- Establish CHANGELOG for version tracking

### Fixed
- Fixed missing qmap_fin() calls in ti_intersect() and split_create() causing memory leaks
- Fixed integer underflow bug in splits_create() when matches_l=0 causing infinite loop and corruption
- Added proper empty result handling in splits_get() for query ranges with no matches

### Added
- Comprehensive test suite (Phase 1 & 2):
  - Category 1: Basic initialization tests (3 tests)
  - Category 2: Basic start/stop operation tests (10 tests)
  - Category 3: Multiple entities tests (15 tests)
  - Category 4: Intersection query tests (8 tests)
  - Category 5: Split computation tests (8 tests)
- Total: 44 automated tests covering all core functionality
- Integration test script (test.sh) with regression testing via expects.txt
- Test output formatting with ✅/❌ indicators for easy visual verification

### Notes
- Code is fully compatible with qmap v0.6.0 (updated in v1.0.0)
- Benefits from qmap v0.6.0 improvements:
  - Improved pointer stability (allocation reuse)
  - Automatic file loading for persistence
  - Enhanced qmap documentation

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
