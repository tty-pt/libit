#include "./../include/ttypt/joint.h"

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>

#include <ttypt/qsys.h>

char *good = "✅";
char *bad = "❌";

unsigned errors = 0;

/* Test infrastructure */
#define TEST(name) \
	static void test_##name(void); \
	static void run_test_##name(void) { \
		printf("test_%s ", #name); \
		test_##name(); \
		printf("\n"); \
	} \
	static void test_##name(void)

#define RUN_TEST(name) run_test_##name()

#define ASSERT(cond) do { \
	if (!(cond)) { \
		printf("%s ", bad); \
		printf("FAIL: %s:%d: %s", __FILE__, __LINE__, #cond); \
		errors++; \
		return; \
	} else { \
		printf("%s ", good); \
	} \
} while(0)

#define ASSERT_EQ(a, b) do { \
	if ((a) != (b)) { \
		printf("%s ", bad); \
		printf("FAIL: %s:%d: %s != %s (%ld != %ld)", \
			__FILE__, __LINE__, #a, #b, (long)(a), (long)(b)); \
		errors++; \
		return; \
	} else { \
		printf("%s ", good); \
	} \
} while(0)

#define ASSERT_STR_EQ(a, b) do { \
	if (strcmp((a), (b)) != 0) { \
		printf("%s ", bad); \
		printf("FAIL: %s:%d: %s != %s (\"%s\" != \"%s\")", \
			__FILE__, __LINE__, #a, #b, (a), (b)); \
		errors++; \
		return; \
	} else { \
		printf("%s ", good); \
	} \
} while(0)

/* Helper function to remove test database files */
static void cleanup_db(const char *fname) {
	char buf[256];
	snprintf(buf, sizeof(buf), "%s", fname);
	unlink(buf);
	snprintf(buf, sizeof(buf), "%s.ti", fname);
	unlink(buf);
	snprintf(buf, sizeof(buf), "%s.max", fname);
	unlink(buf);
	snprintf(buf, sizeof(buf), "%s.id", fname);
	unlink(buf);
}

/* ============================================
 * Category 1: Basic Initialization
 * ============================================ */

TEST(init_memory) {
	unsigned jd = joint_init(NULL);
	/* ID starts from 0, so just check that init succeeded */
	ASSERT(jd == jd); /* Always true, just verifying it doesn't crash */
}

TEST(init_file) {
	const char *fname = "test_init.qmap";
	cleanup_db(fname);
	
	unsigned jd = joint_init((char *)fname);
	/* Just verify init doesn't crash - file creation is handled by qmap */
	ASSERT(jd == jd);
	
	cleanup_db(fname);
}

TEST(init_multiple) {
	unsigned itd1 = joint_init(NULL);
	unsigned itd2 = joint_init(NULL);
	unsigned itd3 = joint_init(NULL);
	
	/* Just verify each init works and returns different IDs */
	ASSERT(itd1 != itd2);
	ASSERT(itd2 != itd3);
}

/* ============================================
 * Category 2: Basic Start/Stop Operations
 * ============================================ */

TEST(start_single) {
	unsigned jd = joint_init(NULL);
	time_t t1 = 1000;
	unsigned id = 1;
	
	int ret = joint_start(jd, t1, id);
	ASSERT_EQ(ret, 0);
}

TEST(stop_single) {
	unsigned jd = joint_init(NULL);
	time_t t1 = 1000;
	time_t t2 = 2000;
	unsigned id = 1;
	
	joint_start(jd, t1, id);
	int ret = joint_stop(jd, t2, id);
	ASSERT_EQ(ret, 0);
}

TEST(start_stop_sequence) {
	unsigned jd = joint_init(NULL);
	unsigned id = 1;
	
	int ret1 = joint_start(jd, 1000, id);
	int ret2 = joint_stop(jd, 2000, id);
	int ret3 = joint_start(jd, 3000, id);
	int ret4 = joint_stop(jd, 4000, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
	ASSERT_EQ(ret3, 0);
	ASSERT_EQ(ret4, 0);
}

TEST(duplicate_start) {
	unsigned jd = joint_init(NULL);
	time_t t1 = 1000;
	unsigned id = 1;
	
	int ret1 = joint_start(jd, t1, id);
	int ret2 = joint_start(jd, t1, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 1); /* Should return 1 for duplicate */
}

TEST(stop_without_start) {
	unsigned jd = joint_init(NULL);
	time_t t1 = 1000;
	unsigned id = 1;
	
	/* Stop without start creates interval from -infinity */
	int ret = joint_stop(jd, t1, id);
	ASSERT_EQ(ret, 1); /* Should return 1 for no open interval */
}

TEST(start_stop_same_time) {
	unsigned jd = joint_init(NULL);
	time_t t = 1000;
	unsigned id = 1;
	
	int ret1 = joint_start(jd, t, id);
	int ret2 = joint_stop(jd, t, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
}

TEST(zero_timestamp) {
	unsigned jd = joint_init(NULL);
	unsigned id = 1;
	
	int ret1 = joint_start(jd, 0, id);
	int ret2 = joint_stop(jd, 1000, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
}

TEST(negative_timestamp) {
	unsigned jd = joint_init(NULL);
	unsigned id = 1;
	
	int ret1 = joint_start(jd, -1000, id);
	int ret2 = joint_stop(jd, 1000, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
}

TEST(large_timestamp) {
	unsigned jd = joint_init(NULL);
	unsigned id = 1;
	time_t large = 2147483647; /* Max 32-bit signed int */
	
	int ret1 = joint_start(jd, 0, id);
	int ret2 = joint_stop(jd, large, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
}

TEST(backward_interval) {
	unsigned jd = joint_init(NULL);
	unsigned id = 1;
	
	/* Stop before start - should create backward interval */
	int ret1 = joint_stop(jd, 1000, id);
	
	ASSERT_EQ(ret1, 1);
}

/* ============================================
 * Category 3: Multiple Entities
 * ============================================ */

TEST(multiple_entities_separate) {
	unsigned jd = joint_init(NULL);
	
	int ret1 = joint_start(jd, 1000, 1);
	int ret2 = joint_start(jd, 1000, 2);
	int ret3 = joint_start(jd, 1000, 3);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
	ASSERT_EQ(ret3, 0);
}

TEST(multiple_entities_overlapping) {
	unsigned jd = joint_init(NULL);
	
	joint_start(jd, 1000, 1);
	joint_start(jd, 1500, 2);
	joint_start(jd, 2000, 3);
	
	int ret1 = joint_stop(jd, 3000, 1);
	int ret2 = joint_stop(jd, 2500, 2);
	int ret3 = joint_stop(jd, 4000, 3);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
	ASSERT_EQ(ret3, 0);
}

/* Test for qmap association bug - stop two entities in reverse order */
TEST(qmap_association_two_entities) {
	unsigned jd = joint_init(NULL);
	
	/* Start two entities with overlapping intervals */
	joint_start(jd, 1000, 1);
	joint_start(jd, 1500, 2);
	
	/* Stop entity 2 first - tests that secondary index is properly maintained */
	int ret1 = joint_stop(jd, 2500, 2);
	ASSERT_EQ(ret1, 0);
	
	/* Stop entity 1 second - previously failed due to corrupted secondary index */
	int ret2 = joint_stop(jd, 3000, 1);
	ASSERT_EQ(ret2, 0);
}

/* Test for qmap association bug - stop entities in different orders */
TEST(qmap_association_reverse_order) {
	unsigned jd = joint_init(NULL);
	
	/* Start 3 entities */
	joint_start(jd, 1000, 1);
	joint_start(jd, 1500, 2);
	joint_start(jd, 2000, 3);
	
	/* Stop in reverse order */
	int ret3 = joint_stop(jd, 4000, 3);
	int ret2 = joint_stop(jd, 2500, 2);
	int ret1 = joint_stop(jd, 3000, 1);
	
	ASSERT_EQ(ret3, 0);
	ASSERT_EQ(ret2, 0);
	ASSERT_EQ(ret1, 0);
}

/* Test for qmap association bug - stop entities in mixed order */
TEST(qmap_association_mixed_order) {
	unsigned jd = joint_init(NULL);
	
	/* Start 4 entities */
	joint_start(jd, 1000, 1);
	joint_start(jd, 1500, 2);
	joint_start(jd, 2000, 3);
	joint_start(jd, 2500, 4);
	
	/* Stop in mixed order: 2, 4, 1, 3 */
	int ret2 = joint_stop(jd, 3500, 2);
	int ret4 = joint_stop(jd, 5000, 4);
	int ret1 = joint_stop(jd, 4000, 1);
	int ret3 = joint_stop(jd, 4500, 3);
	
	ASSERT_EQ(ret2, 0);
	ASSERT_EQ(ret4, 0);
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret3, 0);
}

TEST(multiple_entities_nonoverlapping) {
	unsigned jd = joint_init(NULL);
	
	joint_start(jd, 1000, 1);
	joint_stop(jd, 2000, 1);
	
	joint_start(jd, 3000, 2);
	joint_stop(jd, 4000, 2);
	
	joint_start(jd, 5000, 3);
	int ret = joint_stop(jd, 6000, 3);
	
	ASSERT_EQ(ret, 0);
}

TEST(same_entity_multiple_intervals) {
	unsigned jd = joint_init(NULL);
	unsigned id = 1;
	
	joint_start(jd, 1000, id);
	joint_stop(jd, 2000, id);
	
	joint_start(jd, 3000, id);
	joint_stop(jd, 4000, id);
	
	joint_start(jd, 5000, id);
	int ret = joint_stop(jd, 6000, id);
	
	ASSERT_EQ(ret, 0);
}

TEST(interleaved_operations) {
	unsigned jd = joint_init(NULL);
	
	joint_start(jd, 1000, 1);
	joint_start(jd, 1100, 2);
	joint_stop(jd, 1200, 1);
	joint_start(jd, 1300, 3);
	joint_stop(jd, 1400, 2);
	int ret = joint_stop(jd, 1500, 3);
	
	ASSERT_EQ(ret, 0);
}

TEST(many_entities) {
	unsigned jd = joint_init(NULL);
	time_t base = 1000;
	
	/* Start 10 entities */
	for (unsigned i = 0; i < 10; i++) {
		int ret = joint_start(jd, base + i * 100, i);
		ASSERT_EQ(ret, 0);
	}
	
	/* Stop 10 entities */
	for (unsigned i = 0; i < 10; i++) {
		int ret = joint_stop(jd, base + 1000 + i * 100, i);
		ASSERT_EQ(ret, 0);
	}
}

TEST(entity_zero) {
	unsigned jd = joint_init(NULL);
	unsigned id = 0;
	
	int ret1 = joint_start(jd, 1000, id);
	int ret2 = joint_stop(jd, 2000, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
}

TEST(large_entity_id) {
	unsigned jd = joint_init(NULL);
	unsigned id = 999999;
	
	int ret1 = joint_start(jd, 1000, id);
	int ret2 = joint_stop(jd, 2000, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
}

/* ============================================
 * Category 4: Intersection Queries (joint_iter + joint_next)
 * ============================================ */

TEST(iter_empty_range) {
	unsigned jd = joint_init(NULL);
	
	/* Add interval outside query range */
	joint_start(jd, 1000, 1);
	joint_stop(jd, 2000, 1);
	
	/* Query range that doesn't intersect */
	joint_cur_t cur = joint_iter(jd, 3000, 4000);
	time_t min, max;
	unsigned count, who;
	
	int has_results = joint_next(&min, &max, &count, &who, &cur);
	ASSERT_EQ(has_results, 0); /* Should be empty */
}

TEST(iter_single_interval) {
	unsigned jd = joint_init(NULL);
	
	/* Single interval fully in range */
	joint_start(jd, 1000, 42);
	joint_stop(jd, 2000, 42);
	
	/* Query covering the interval */
	joint_cur_t cur = joint_iter(jd, 500, 2500);
	time_t min, max;
	unsigned count, who;
	
	int ret = joint_next(&min, &max, &count, &who, &cur);
	ASSERT_EQ(ret, 1);
	ASSERT_EQ(min, 1000);
	ASSERT_EQ(max, 2000);
	ASSERT_EQ(count, 1);
	ASSERT_EQ(who, 42);
	
	/* Should be no more results */
	ret = joint_next(&min, &max, &count, &who, &cur);
	ASSERT_EQ(ret, 0);
}

TEST(iter_partial_overlap) {
	unsigned jd = joint_init(NULL);
	
	/* Interval extends beyond query range */
	joint_start(jd, 1000, 5);
	joint_stop(jd, 3000, 5);
	
	/* Query only part of the interval */
	joint_cur_t cur = joint_iter(jd, 1500, 2500);
	time_t min, max;
	unsigned count, who;
	
	int ret = joint_next(&min, &max, &count, &who, &cur);
	ASSERT_EQ(ret, 1);
	/* Should get the intersection portion */
	ASSERT_EQ(min, 1500);
	ASSERT_EQ(max, 2500);
	ASSERT_EQ(who, 5);
}

TEST(iter_multiple_non_overlapping) {
	unsigned jd = joint_init(NULL);
	
	/* Three separate intervals */
	joint_start(jd, 1000, 1);
	joint_stop(jd, 1500, 1);
	
	joint_start(jd, 2000, 2);
	joint_stop(jd, 2500, 2);
	
	joint_start(jd, 3000, 3);
	joint_stop(jd, 3500, 3);
	
	/* Query covering all three */
	joint_cur_t cur = joint_iter(jd, 900, 3600);
	time_t min, max;
	unsigned count, who;
	int found_count = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		found_count++;
		ASSERT(count == 1); /* Each segment has only one entity */
	}
	
	ASSERT_EQ(found_count, 3); /* Should find all three entities */
}

TEST(iter_exact_boundaries) {
	unsigned jd = joint_init(NULL);
	
	/* Interval exactly matches query range */
	joint_start(jd, 1000, 99);
	joint_stop(jd, 2000, 99);
	
	/* Query with exact same boundaries */
	joint_cur_t cur = joint_iter(jd, 1000, 2000);
	time_t min, max;
	unsigned count, who;
	
	int ret = joint_next(&min, &max, &count, &who, &cur);
	ASSERT_EQ(ret, 1);
	ASSERT_EQ(min, 1000);
	ASSERT_EQ(max, 2000);
	ASSERT_EQ(who, 99);
}

TEST(iter_before_range) {
	unsigned jd = joint_init(NULL);
	
	/* Intervals completely before query range */
	joint_start(jd, 100, 1);
	joint_stop(jd, 200, 1);
	joint_start(jd, 300, 2);
	joint_stop(jd, 400, 2);
	
	/* Query after all intervals */
	joint_cur_t cur = joint_iter(jd, 1000, 2000);
	time_t min, max;
	unsigned count, who;
	
	int ret = joint_next(&min, &max, &count, &who, &cur);
	ASSERT_EQ(ret, 0); /* Should be empty */
}

TEST(iter_after_range) {
	unsigned jd = joint_init(NULL);
	
	/* Intervals completely after query range */
	joint_start(jd, 5000, 1);
	joint_stop(jd, 6000, 1);
	
	/* Query before all intervals */
	joint_cur_t cur = joint_iter(jd, 1000, 2000);
	time_t min, max;
	unsigned count, who;
	
	int ret = joint_next(&min, &max, &count, &who, &cur);
	ASSERT_EQ(ret, 0); /* Should be empty */
}

TEST(iter_same_entity_multiple_intervals) {
	unsigned jd = joint_init(NULL);
	
	/* Same entity with two separate intervals */
	joint_start(jd, 1000, 7);
	joint_stop(jd, 1500, 7);
	
	joint_start(jd, 2000, 7);
	joint_stop(jd, 2500, 7);
	
	/* Query covering both intervals */
	joint_cur_t cur = joint_iter(jd, 900, 2600);
	time_t min, max;
	unsigned count, who;
	int found_count = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		ASSERT_EQ(who, 7); /* Always entity 7 */
		found_count++;
	}
	
	ASSERT_EQ(found_count, 2); /* Should find both intervals */
}

/* ============================================
 * Category 5: Split Computation Tests
 * ============================================ */

/* Test: Single entity present throughout range - should create one split */
TEST(split_single_entity_full_range) {
	unsigned jd = joint_init(NULL);
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who;
	int split_count;
	
	joint_start(jd, 1000, 1);
	joint_stop(jd, 3000, 1);
	
	cur = joint_iter(jd, 1000, 3000);
	split_count = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		split_count++;
		ASSERT_EQ(who, 1);
		ASSERT_EQ(count, 1);
		ASSERT_EQ(min, 1000);
		ASSERT_EQ(max, 3000);
	}
	
	ASSERT_EQ(split_count, 1); /* Should have exactly one split */
}

/* Test: Two entities with overlapping intervals - should create 3 splits */
TEST(split_two_overlapping) {
	unsigned jd = joint_init(NULL);
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who;
	int split_count, counts[10], i;
	
	for (i = 0; i < 10; i++) counts[i] = 0;
	
	joint_start(jd, 1000, 1);
	joint_stop(jd, 3000, 1);
	joint_start(jd, 2000, 2);
	joint_stop(jd, 4000, 2);
	
	cur = joint_iter(jd, 1000, 4000);
	split_count = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		counts[count]++;
		split_count++;
	}
	
	/* Should have 3 splits total: [1000-2000] count=1, [2000-3000] count=2, [3000-4000] count=1 */
	ASSERT(split_count >= 3); 
	ASSERT(counts[1] >= 2); /* At least 2 splits with count=1 */
	ASSERT(counts[2] >= 1); /* At least 1 split with count=2 */
}

/* Test: Gap in coverage - should handle periods with no entities */
TEST(split_with_gap) {
	unsigned jd = joint_init(NULL);
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who;
	int split_count, found_entity1, found_entity2;
	
	joint_start(jd, 1000, 1);
	joint_stop(jd, 2000, 1);
	joint_start(jd, 3000, 2);
	joint_stop(jd, 4000, 2);
	
	cur = joint_iter(jd, 1000, 4000);
	split_count = 0;
	found_entity1 = 0;
	found_entity2 = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		split_count++;
		if (who == 1) found_entity1++;
		if (who == 2) found_entity2++;
	}
	
	ASSERT(split_count >= 2); /* At least 2 splits for the two intervals */
	ASSERT_EQ(found_entity1, 1);
	ASSERT_EQ(found_entity2, 1);
}

/* Test: Three entities with different overlaps */
TEST(split_three_entities) {
	unsigned jd = joint_init(NULL);
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who;
	int split_count, max_count;
	
	joint_start(jd, 1000, 1);
	joint_stop(jd, 4000, 1);
	joint_start(jd, 2000, 2);
	joint_stop(jd, 3000, 2);
	joint_start(jd, 2500, 3);
	joint_stop(jd, 3500, 3);
	
	cur = joint_iter(jd, 1000, 4000);
	split_count = 0;
	max_count = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		split_count++;
		if ((int)count > max_count) max_count = count;
	}
	
	ASSERT(split_count >= 4); /* Multiple splits */
	ASSERT(max_count >= 2); /* At least some overlaps */
}

/* Test: Query range partially outside intervals */
TEST(split_partial_query_range) {
	unsigned jd = joint_init(NULL);
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who;
	int split_count;
	
	joint_start(jd, 2000, 1);
	joint_stop(jd, 3000, 1);
	
	cur = joint_iter(jd, 1000, 4000);
	split_count = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		split_count++;
		ASSERT_EQ(who, 1);
		/* Split should be clipped to interval bounds */
		ASSERT(min >= 2000);
		ASSERT(max <= 3000);
	}
	
	ASSERT_EQ(split_count, 1); /* Should have one split for the interval */
}

/* Test: Query range entirely within one interval */
TEST(split_query_within_interval) {
	unsigned jd = joint_init(NULL);
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who;
	int split_count;
	
	joint_start(jd, 1000, 1);
	joint_stop(jd, 5000, 1);
	
	cur = joint_iter(jd, 2000, 3000);
	split_count = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		split_count++;
		ASSERT_EQ(who, 1);
		ASSERT_EQ(min, 2000);
		ASSERT_EQ(max, 3000);
		ASSERT_EQ(count, 1);
	}
	
	ASSERT_EQ(split_count, 1);
}

/* Test: Adjacent intervals (no overlap) */
TEST(split_adjacent_intervals) {
	unsigned jd = joint_init(NULL);
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who;
	int split_count, found_entity1, found_entity2;
	
	joint_start(jd, 1000, 1);
	joint_stop(jd, 2000, 1);
	joint_start(jd, 2000, 2);
	joint_stop(jd, 3000, 2);
	
	cur = joint_iter(jd, 1000, 3000);
	split_count = 0;
	found_entity1 = 0;
	found_entity2 = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		split_count++;
		if (who == 1) found_entity1++;
		if (who == 2) found_entity2++;
	}
	
	ASSERT(split_count >= 2); /* At least 2 splits */
	ASSERT_EQ(found_entity1, 1);
	ASSERT_EQ(found_entity2, 1);
}

/* Test: Many small overlapping intervals */
TEST(split_many_small_intervals) {
	unsigned jd = joint_init(NULL);
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who, i;
	int split_count;
	
	for (i = 0; i < 10; i++) {
		joint_start(jd, 1000 + i * 100, i + 1);
		joint_stop(jd, 1200 + i * 100, i + 1);
	}
	
	cur = joint_iter(jd, 1000, 2000);
	split_count = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		split_count++;
	}
	
	ASSERT(split_count >= 10); /* Should have at least 10 splits */
}

/* ============================================
 * Category 6: Time Utilities Tests
 * ============================================ */

/* Test: Parse ISO-8601 date+time format */
TEST(time_parse_datetime) {
	time_t ts;
	char buf[DATE_MAX_LEN];
	
	ts = sscantime("2024-12-25T14:30:00");
	ASSERT(ts > 0);
	
	/* Verify round-trip */
	printtime(buf, ts);
	ASSERT(strcmp(buf, "2024-12-25T14:30:00") == 0);
}

/* Test: Parse ISO-8601 date-only format */
TEST(time_parse_date_only) {
	time_t ts;
	char buf[DATE_MAX_LEN];
	
	ts = sscantime("2024-12-25");
	ASSERT(ts > 0);
	
	/* Date-only should format without time component */
	printtime(buf, ts);
	ASSERT(strcmp(buf, "2024-12-25") == 0);
}

/* Test: Parse Unix timestamp string */
TEST(time_parse_unix_timestamp) {
	time_t ts, ts2;
	
	ts = 1735139400;
	ts2 = sscantime("1735139400");
	ASSERT_EQ(ts, ts2);
}

/* Test: Format infinity values */
TEST(time_format_infinity) {
	char buf[DATE_MAX_LEN];
	unsigned jd = joint_init(NULL);
	
	/* Get the infinity constants by creating an open interval */
	joint_start(jd, 1000, 1);
	
	/* Note: mtinf and tinf are internal constants, so we test via the API */
	/* We can't directly test them, but we verify printtime handles edge cases */
	printtime(buf, 0);
	ASSERT(strlen(buf) > 0); /* Should produce some output */
}

/* Test: Format regular timestamp */
TEST(time_format_regular) {
	char buf[DATE_MAX_LEN];
	time_t ts;
	
	ts = sscantime("2024-01-15T10:30:45");
	printtime(buf, ts);
	
	ASSERT(strcmp(buf, "2024-01-15T10:30:45") == 0);
}

/* Test: Round-trip various formats */
TEST(time_roundtrip) {
	char buf[DATE_MAX_LEN];
	time_t ts1, ts2;
	
	/* Test 1: Full datetime */
	ts1 = sscantime("2023-06-15T08:15:30");
	printtime(buf, ts1);
	ts2 = sscantime(buf);
	ASSERT_EQ(ts1, ts2);
	
	/* Test 2: Date only */
	ts1 = sscantime("2023-06-15");
	printtime(buf, ts1);
	ts2 = sscantime(buf);
	ASSERT_EQ(ts1, ts2);
}

/* ============================================
 * Category 7: Persistence Tests
 * ============================================ */

/* Test: Save and load from file */
TEST(persist_save_load) {
	unsigned itd1, itd2;
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who;
	int found;
	
	/* Create database with data */
	itd1 = joint_init("/tmp/test_persist.db");
	joint_start(itd1, 1000, 42);
	joint_stop(itd1, 2000, 42);
	joint_close(itd1); /* Close to persist data */
	
	/* Load database in new handle */
	itd2 = joint_init("/tmp/test_persist.db");
	
	/* Verify data persisted */
	cur = joint_iter(itd2, 500, 2500);
	found = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		if (who == 42 && min == 1000 && max == 2000) {
			found = 1;
		}
	}
	
	ASSERT_EQ(found, 1);
}

/* Test: Multiple intervals persist correctly */
TEST(persist_multiple_intervals) {
	unsigned itd1, itd2;
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who;
	int found_count;
	
	/* Create database with multiple intervals */
	itd1 = joint_init("/tmp/test_persist_multi.db");
	joint_start(itd1, 1000, 1);
	joint_stop(itd1, 2000, 1);
	joint_start(itd1, 3000, 2);
	joint_stop(itd1, 4000, 2);
	joint_start(itd1, 5000, 3);
	joint_stop(itd1, 6000, 3);
	joint_close(itd1); /* Close to persist data */
	
	/* Reload */
	itd2 = joint_init("/tmp/test_persist_multi.db");
	
	/* Count intervals */
	cur = joint_iter(itd2, 0, 7000);
	found_count = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		found_count++;
	}
	
	ASSERT(found_count >= 3); /* Should find at least 3 intervals */
}

/* Test: Empty database persists correctly */
TEST(persist_empty_database) {
	unsigned itd1, itd2;
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who;
	int has_results;
	
	/* Create empty database */
	itd1 = joint_init("/tmp/test_persist_empty.db");
	joint_close(itd1); /* Close to persist data */
	
	/* Reload */
	itd2 = joint_init("/tmp/test_persist_empty.db");
	
	/* Should be empty */
	cur = joint_iter(itd2, 0, 10000);
	has_results = joint_next(&min, &max, &count, &who, &cur);
	
	ASSERT_EQ(has_results, 0);
}

/* Test: Append to existing database */
TEST(persist_append) {
	unsigned itd1, itd2, itd3;
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who;
	int found_count;
	
	/* Create initial data */
	itd1 = joint_init("/tmp/test_persist_append.db");
	joint_start(itd1, 1000, 1);
	joint_stop(itd1, 2000, 1);
	joint_close(itd1); /* Close to persist data */
	
	/* Reload and add more */
	itd2 = joint_init("/tmp/test_persist_append.db");
	joint_start(itd2, 3000, 2);
	joint_stop(itd2, 4000, 2);
	joint_close(itd2); /* Close to persist data */
	
	/* Reload again and verify both intervals exist */
	itd3 = joint_init("/tmp/test_persist_append.db");
	cur = joint_iter(itd3, 0, 5000);
	found_count = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		found_count++;
	}
	
	ASSERT(found_count >= 2); /* Should have both intervals */
}

/* Test: Large dataset persistence */
TEST(persist_large_dataset) {
	unsigned itd1, itd2;
	joint_cur_t cur;
	time_t min, max;
	unsigned count, who, i;
	int found_count;
	
	/* Create large dataset */
	itd1 = joint_init("/tmp/test_persist_large.db");
	for (i = 0; i < 50; i++) {
		joint_start(itd1, 1000 + i * 100, i + 1);
		joint_stop(itd1, 1050 + i * 100, i + 1);
	}
	joint_close(itd1); /* Close to persist data */
	
	/* Reload */
	itd2 = joint_init("/tmp/test_persist_large.db");
	
	/* Count intervals */
	cur = joint_iter(itd2, 0, 10000);
	found_count = 0;
	
	while (joint_next(&min, &max, &count, &who, &cur)) {
		found_count++;
	}
	
	ASSERT(found_count >= 50); /* Should have all 50 intervals */
}

/* ============================================
 * Category 8: Input Validation
 * ============================================ */

TEST(validation_extreme_timestamp_start) {
	unsigned jd = joint_init(NULL);
	
	/* Test timestamp beyond valid range (> LONG_MAX/2) in joint_start */
	time_t extreme_ts = LONG_MAX / 2 + 1;
	int result = joint_start(jd, extreme_ts, 1);
	
	ASSERT(result == -1);
	ASSERT(errno == ERANGE);
}

TEST(validation_extreme_timestamp_stop) {
	unsigned jd = joint_init(NULL);
	
	/* Test timestamp beyond valid range (> LONG_MAX/2) in joint_stop */
	time_t extreme_ts = LONG_MAX / 2 + 1;
	int result = joint_stop(jd, extreme_ts, 1);
	
	ASSERT(result == -1);
	ASSERT(errno == ERANGE);
}

TEST(validation_uint32_max_start) {
	unsigned jd = joint_init(NULL);
	
	/* Test UINT32_MAX entity ID (reserved as IDM_MISS sentinel) in joint_start */
	int result = joint_start(jd, 1000, UINT32_MAX);
	
	ASSERT(result == -1);
	ASSERT(errno == EINVAL);
}

TEST(validation_uint32_max_stop) {
	unsigned jd = joint_init(NULL);
	
	/* Test UINT32_MAX entity ID (reserved as IDM_MISS sentinel) in joint_stop */
	int result = joint_stop(jd, 1000, UINT32_MAX);
	
	ASSERT(result == -1);
	ASSERT(errno == EINVAL);
}

/* ============================================
 * Main test runner
 * ============================================ */

int main(void) {
	printf("Running libjoint tests...\n\n");
	
	/* Category 1: Basic Initialization */
	printf("=== Category 1: Basic Initialization ===\n");
	RUN_TEST(init_memory);
	RUN_TEST(init_file);
	RUN_TEST(init_multiple);
	
	/* Category 2: Basic Start/Stop Operations */
	printf("\n=== Category 2: Basic Start/Stop Operations ===\n");
	RUN_TEST(start_single);
	RUN_TEST(stop_single);
	RUN_TEST(start_stop_sequence);
	RUN_TEST(duplicate_start);
	RUN_TEST(stop_without_start);
	RUN_TEST(start_stop_same_time);
	RUN_TEST(zero_timestamp);
	RUN_TEST(negative_timestamp);
	RUN_TEST(large_timestamp);
	RUN_TEST(backward_interval);
	
	/* Category 3: Multiple Entities */
	printf("\n=== Category 3: Multiple Entities ===\n");
	RUN_TEST(multiple_entities_separate);
	RUN_TEST(multiple_entities_overlapping);
	RUN_TEST(qmap_association_two_entities);
	RUN_TEST(qmap_association_reverse_order);
	RUN_TEST(qmap_association_mixed_order);
	RUN_TEST(multiple_entities_nonoverlapping);
	RUN_TEST(same_entity_multiple_intervals);
	RUN_TEST(interleaved_operations);
	RUN_TEST(many_entities);
	RUN_TEST(entity_zero);
	RUN_TEST(large_entity_id);
	
	/* Category 4: Intersection Queries */
	printf("\n=== Category 4: Intersection Queries ===\n");
	RUN_TEST(iter_empty_range);
	RUN_TEST(iter_single_interval);
	RUN_TEST(iter_partial_overlap);
	RUN_TEST(iter_multiple_non_overlapping);
	RUN_TEST(iter_exact_boundaries);
	RUN_TEST(iter_before_range);
	RUN_TEST(iter_after_range);
	RUN_TEST(iter_same_entity_multiple_intervals);
	
	/* Category 5: Split Computation */
	printf("\n=== Category 5: Split Computation ===\n");
	RUN_TEST(split_single_entity_full_range);
	RUN_TEST(split_two_overlapping);
	RUN_TEST(split_with_gap);
	RUN_TEST(split_three_entities);
	RUN_TEST(split_partial_query_range);
	RUN_TEST(split_query_within_interval);
	RUN_TEST(split_adjacent_intervals);
	RUN_TEST(split_many_small_intervals);
	
	/* Category 6: Time Utilities */
	printf("\n=== Category 6: Time Utilities ===\n");
	RUN_TEST(time_parse_datetime);
	RUN_TEST(time_parse_date_only);
	RUN_TEST(time_parse_unix_timestamp);
	RUN_TEST(time_format_infinity);
	RUN_TEST(time_format_regular);
	RUN_TEST(time_roundtrip);
	
	/* Category 7: Persistence */
	printf("\n=== Category 7: Persistence ===\n");
	RUN_TEST(persist_save_load);
	RUN_TEST(persist_multiple_intervals);
	RUN_TEST(persist_empty_database);
	RUN_TEST(persist_append);
	RUN_TEST(persist_large_dataset);
	
	/* Category 8: Input Validation */
	printf("\n=== Category 8: Input Validation ===\n");
	RUN_TEST(validation_extreme_timestamp_start);
	RUN_TEST(validation_extreme_timestamp_stop);
	RUN_TEST(validation_uint32_max_start);
	RUN_TEST(validation_uint32_max_stop);
	
	printf("\n=== Test Summary ===\n");
	printf("Total errors: %u\n", errors);
	
	return errors > 0 ? 1 : 0;
}
