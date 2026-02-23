#ifndef IT_H
#define IT_H

/**
 * @file it.h
 * @brief Public API for the Interval Tree Library (libit).
 *
 * Provides an efficient interval tree implementation for tracking
 * time-based intervals with persistence support. Built on libqmap
 * for high-performance querying and sorted iteration.
 *
 * Key features:
 * - Store and query time intervals with associated IDs
 * - File-based persistence via qmap
 * - Efficient intersection queries
 * - Multiple secondary indexes (by max time, by ID)
 * - Sorted iteration support
 *
 * @see qmap.h for underlying storage implementation
 */

#include <time.h>
#include <sys/types.h>

/**
 * @brief Maximum length for ISO-8601 date string buffers.
 *
 * Used by printtime() for formatting timestamps.
 */
#define DATE_MAX_LEN 20

/**
 * @brief Opaque iterator handle for interval tree traversal.
 *
 * Created by it_iter() and used with it_next() to traverse
 * intervals that intersect a time range. Must not be freed
 * directly by the user.
 *
 * @see it_iter
 * @see it_next
 */
typedef void * it_cur_t;

/**
 * @brief Initialize an interval tree database.
 *
 * Creates or opens a file-backed interval tree with three
 * internal qmap databases:
 * - "ti": Primary map (interval -> interval)
 * - "max": Secondary index sorted by interval max time
 * - "id": Secondary index sorted by entity ID
 *
 * @param[in] fname Path to database file, or NULL for in-memory only.
 *                  If provided, data persists across program runs.
 *
 * @return Database handle for use with other it_* functions.
 *         Handle is an integer ID that remains valid until
 *         process exit (no explicit close needed).
 *
 * @note File persistence uses qmap's automatic save-on-exit.
 *       Multiple databases can share one file via different names.
 *
 * @see it_start
 * @see it_stop
 * @see it_iter
 *
 * Example:
 * @code
 * // Create persistent interval tree
 * uint32_t itd = it_init("events.qmap");
 * 
 * // Add intervals
 * it_start(itd, timestamp1, user_id);
 * it_stop(itd, timestamp2, user_id);
 * 
 * // Query overlapping intervals
 * it_cur_t cur = it_iter(itd, start_time, end_time);
 * time_t min, max;
 * uint32_t count, who;
 * while (it_next(&min, &max, &count, &who, &cur)) {
 *     printf("Interval: %ld-%ld, ID: %u\n", min, max, who);
 * }
 * @endcode
 */
unsigned it_init(char *fname);

/**
 * @brief Close an interval tree database and free resources.
 *
 * Closes all associated qmap databases for the given handle,
 * ensuring data is persisted to disk. After calling this function,
 * the handle should not be used again.
 *
 * @param[in] itd Database handle from it_init().
 *
 * @note This function should be called before re-opening the same
 *       database file to ensure data persistence.
 *
 * @see it_init
 */
void it_close(unsigned itd);

/**
 * @brief Start a new interval for an entity.
 *
 * Records that an entity (identified by id) began an activity
 * at the given timestamp. The interval remains open (max = infinity)
 * until closed with it_stop().
 *
 * @param[in] itd Database handle from it_init().
 * @param[in] ts  Timestamp when the interval begins.
 *               Must be in range [LONG_MIN/2, LONG_MAX/2] to avoid
 *               conflicts with internal sentinel values.
 * @param[in] id  Entity identifier (e.g., user ID, session ID).
 *               Cannot be UINT32_MAX (reserved as internal sentinel).
 *
 * @return 0 on success (new interval started).
 *         1 if entity already has an open interval at this time
 *         (duplicate start attempt).
 *         -1 on validation error (check errno):
 *           - ERANGE: timestamp outside valid range
 *           - EINVAL: entity ID is UINT32_MAX
 *
 * @note If an entity already has an open interval, this function
 *       returns 1 and does not create a duplicate.
 *
 * @see it_stop
 * @see it_init
 */
int it_start(unsigned itd, time_t ts, unsigned id);

/**
 * @brief Stop an interval for an entity.
 *
 * Closes the most recent open interval for the given entity,
 * setting its end time to the provided timestamp. If no open
 * interval exists, creates a new interval ending at this time
 * with start = -infinity.
 *
 * @param[in] itd Database handle from it_init().
 * @param[in] ts  Timestamp when the interval ends.
 *               Must be in range [LONG_MIN/2, LONG_MAX/2] to avoid
 *               conflicts with internal sentinel values.
 * @param[in] id  Entity identifier.
 *               Cannot be UINT32_MAX (reserved as internal sentinel).
 *
 * @return 0 on success (existing interval closed).
 *         1 if no open interval existed (created backward interval).
 *         -1 on validation error (check errno):
 *           - ERANGE: timestamp outside valid range
 *           - EINVAL: entity ID is UINT32_MAX
 *
 * @note This function handles the case where stop is called
 *       before start by creating an interval from -infinity.
 *
 * @see it_start
 * @see it_init
 */
int it_stop(unsigned itd, time_t ts, unsigned id);

/**
 * @brief Begin iterating over intervals that intersect a time range.
 *
 * Creates an iterator for all intervals that overlap with the
 * specified time range [start, end). The iterator returns split
 * intervals that show which entities were present during each
 * sub-interval.
 *
 * @param[in] itd   Database handle from it_init().
 * @param[in] start Start of the query time range (inclusive).
 * @param[in] end   End of the query time range (exclusive).
 *
 * @return Opaque iterator handle for use with it_next().
 *         The iterator remains valid until all results are consumed
 *         or the process exits. No explicit cleanup needed.
 *
 * @note The iterator computes split intervals that show periods
 *       where the set of present entities changes. Use it_next()
 *       to retrieve each split interval and its associated entities.
 *
 * @see it_next
 * @see it_init
 * @see it_start
 * @see it_stop
 */
it_cur_t it_iter(unsigned itd, time_t start, time_t end);

/**
 * @brief Get the next interval and entity from an iterator.
 *
 * Retrieves the next split interval from the iterator, along with
 * one entity ID that was present during that interval. Call repeatedly
 * to get all entities for each interval.
 *
 * @param[out] min   Start time of this interval segment.
 * @param[out] max   End time of this interval segment.
 * @param[out] count Total number of entities present in this segment.
 * @param[out] who   Entity ID for this iteration.
 * @param[in,out] c  Iterator handle from it_iter().
 *
 * @return 1 if an entity was retrieved (continue iteration).
 *         0 if no more entities/intervals (iteration complete).
 *
 * @note To get all entities for each interval, keep calling it_next()
 *       until count entities are retrieved for that time segment.
 *       The iterator automatically advances to the next interval
 *       when all entities are consumed.
 *
 * @see it_iter
 */
int it_next(time_t *min, time_t *max, unsigned *count, unsigned *who, it_cur_t *c);

/**
 * @brief Parse an ISO-8601 date string or Unix timestamp.
 *
 * Converts a date/time string to a Unix timestamp (time_t).
 * Supports multiple formats:
 * - ISO-8601 date+time: "2024-12-25T14:30:00"
 * - ISO-8601 date only: "2024-12-25"
 * - Unix timestamp: "1735139400"
 *
 * @param[in] buf String containing date/time to parse.
 *
 * @return Parsed timestamp as time_t.
 *         On error, the behavior depends on the input format.
 *
 * @note For date-only format, time is assumed to be 00:00:00.
 *       The function uses the system's local timezone.
 *
 * @see printtime
 */
time_t sscantime(char *buf);

/**
 * @brief Format a Unix timestamp as an ISO-8601 date string.
 *
 * Converts a time_t value to a human-readable date string.
 * Special values -infinity and +infinity are formatted as
 * "-inf" and "inf" respectively.
 *
 * Output format depends on the time component:
 * - If time is 00:00:00: "YYYY-MM-DD" (date only)
 * - Otherwise: "YYYY-MM-DDTHH:MM:SS" (full ISO-8601)
 *
 * @param[out] buf Buffer to store formatted string (must be
 *                 at least DATE_MAX_LEN bytes).
 * @param[in]  ts  Timestamp to format.
 *
 * @note The output uses the system's local timezone.
 *       Special handling for infinity values (min/max time_t).
 *
 * @see sscantime
 * @see DATE_MAX_LEN
 */
void printtime(char buf[DATE_MAX_LEN], time_t ts);

#endif
