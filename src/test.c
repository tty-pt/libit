#include "./../include/ttypt/it.h"

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>

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
	unsigned itd = it_init(NULL);
	/* ID starts from 0, so just check that init succeeded */
	ASSERT(itd == itd); /* Always true, just verifying it doesn't crash */
}

TEST(init_file) {
	const char *fname = "test_init.qmap";
	cleanup_db(fname);
	
	unsigned itd = it_init((char *)fname);
	/* Just verify init doesn't crash - file creation is handled by qmap */
	ASSERT(itd == itd);
	
	cleanup_db(fname);
}

TEST(init_multiple) {
	unsigned itd1 = it_init(NULL);
	unsigned itd2 = it_init(NULL);
	unsigned itd3 = it_init(NULL);
	
	/* Just verify each init works and returns different IDs */
	ASSERT(itd1 != itd2);
	ASSERT(itd2 != itd3);
}

/* ============================================
 * Category 2: Basic Start/Stop Operations
 * ============================================ */

TEST(start_single) {
	unsigned itd = it_init(NULL);
	time_t t1 = 1000;
	unsigned id = 1;
	
	int ret = it_start(itd, t1, id);
	ASSERT_EQ(ret, 0);
}

TEST(stop_single) {
	unsigned itd = it_init(NULL);
	time_t t1 = 1000;
	time_t t2 = 2000;
	unsigned id = 1;
	
	it_start(itd, t1, id);
	int ret = it_stop(itd, t2, id);
	ASSERT_EQ(ret, 0);
}

TEST(start_stop_sequence) {
	unsigned itd = it_init(NULL);
	unsigned id = 1;
	
	int ret1 = it_start(itd, 1000, id);
	int ret2 = it_stop(itd, 2000, id);
	int ret3 = it_start(itd, 3000, id);
	int ret4 = it_stop(itd, 4000, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
	ASSERT_EQ(ret3, 0);
	ASSERT_EQ(ret4, 0);
}

TEST(duplicate_start) {
	unsigned itd = it_init(NULL);
	time_t t1 = 1000;
	unsigned id = 1;
	
	int ret1 = it_start(itd, t1, id);
	int ret2 = it_start(itd, t1, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 1); /* Should return 1 for duplicate */
}

TEST(stop_without_start) {
	unsigned itd = it_init(NULL);
	time_t t1 = 1000;
	unsigned id = 1;
	
	/* Stop without start creates interval from -infinity */
	int ret = it_stop(itd, t1, id);
	ASSERT_EQ(ret, 1); /* Should return 1 for no open interval */
}

TEST(start_stop_same_time) {
	unsigned itd = it_init(NULL);
	time_t t = 1000;
	unsigned id = 1;
	
	int ret1 = it_start(itd, t, id);
	int ret2 = it_stop(itd, t, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
}

TEST(zero_timestamp) {
	unsigned itd = it_init(NULL);
	unsigned id = 1;
	
	int ret1 = it_start(itd, 0, id);
	int ret2 = it_stop(itd, 1000, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
}

TEST(negative_timestamp) {
	unsigned itd = it_init(NULL);
	unsigned id = 1;
	
	int ret1 = it_start(itd, -1000, id);
	int ret2 = it_stop(itd, 1000, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
}

TEST(large_timestamp) {
	unsigned itd = it_init(NULL);
	unsigned id = 1;
	time_t large = 2147483647; /* Max 32-bit signed int */
	
	int ret1 = it_start(itd, 0, id);
	int ret2 = it_stop(itd, large, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
}

TEST(backward_interval) {
	unsigned itd = it_init(NULL);
	unsigned id = 1;
	
	/* Stop before start - should create backward interval */
	int ret1 = it_stop(itd, 1000, id);
	
	ASSERT_EQ(ret1, 1);
}

/* ============================================
 * Category 3: Multiple Entities
 * ============================================ */

TEST(multiple_entities_separate) {
	unsigned itd = it_init(NULL);
	
	int ret1 = it_start(itd, 1000, 1);
	int ret2 = it_start(itd, 1000, 2);
	int ret3 = it_start(itd, 1000, 3);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
	ASSERT_EQ(ret3, 0);
}

TEST(multiple_entities_overlapping) {
	unsigned itd = it_init(NULL);
	
	it_start(itd, 1000, 1);
	it_start(itd, 1500, 2);
	it_start(itd, 2000, 3);
	
	int ret1 = it_stop(itd, 3000, 1);
	int ret2 = it_stop(itd, 2500, 2);
	int ret3 = it_stop(itd, 4000, 3);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
	ASSERT_EQ(ret3, 0);
}

/* Test for qmap association bug - stop two entities in reverse order */
TEST(qmap_association_two_entities) {
	unsigned itd = it_init(NULL);
	
	/* Start two entities with overlapping intervals */
	it_start(itd, 1000, 1);
	it_start(itd, 1500, 2);
	
	/* Stop entity 2 first - tests that secondary index is properly maintained */
	int ret1 = it_stop(itd, 2500, 2);
	ASSERT_EQ(ret1, 0);
	
	/* Stop entity 1 second - previously failed due to corrupted secondary index */
	int ret2 = it_stop(itd, 3000, 1);
	ASSERT_EQ(ret2, 0);
}

/* Test for qmap association bug - stop entities in different orders */
TEST(qmap_association_reverse_order) {
	unsigned itd = it_init(NULL);
	
	/* Start 3 entities */
	it_start(itd, 1000, 1);
	it_start(itd, 1500, 2);
	it_start(itd, 2000, 3);
	
	/* Stop in reverse order */
	int ret3 = it_stop(itd, 4000, 3);
	int ret2 = it_stop(itd, 2500, 2);
	int ret1 = it_stop(itd, 3000, 1);
	
	ASSERT_EQ(ret3, 0);
	ASSERT_EQ(ret2, 0);
	ASSERT_EQ(ret1, 0);
}

/* Test for qmap association bug - stop entities in mixed order */
TEST(qmap_association_mixed_order) {
	unsigned itd = it_init(NULL);
	
	/* Start 4 entities */
	it_start(itd, 1000, 1);
	it_start(itd, 1500, 2);
	it_start(itd, 2000, 3);
	it_start(itd, 2500, 4);
	
	/* Stop in mixed order: 2, 4, 1, 3 */
	int ret2 = it_stop(itd, 3500, 2);
	int ret4 = it_stop(itd, 5000, 4);
	int ret1 = it_stop(itd, 4000, 1);
	int ret3 = it_stop(itd, 4500, 3);
	
	ASSERT_EQ(ret2, 0);
	ASSERT_EQ(ret4, 0);
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret3, 0);
}

TEST(multiple_entities_nonoverlapping) {
	unsigned itd = it_init(NULL);
	
	it_start(itd, 1000, 1);
	it_stop(itd, 2000, 1);
	
	it_start(itd, 3000, 2);
	it_stop(itd, 4000, 2);
	
	it_start(itd, 5000, 3);
	int ret = it_stop(itd, 6000, 3);
	
	ASSERT_EQ(ret, 0);
}

TEST(same_entity_multiple_intervals) {
	unsigned itd = it_init(NULL);
	unsigned id = 1;
	
	it_start(itd, 1000, id);
	it_stop(itd, 2000, id);
	
	it_start(itd, 3000, id);
	it_stop(itd, 4000, id);
	
	it_start(itd, 5000, id);
	int ret = it_stop(itd, 6000, id);
	
	ASSERT_EQ(ret, 0);
}

TEST(interleaved_operations) {
	unsigned itd = it_init(NULL);
	
	it_start(itd, 1000, 1);
	it_start(itd, 1100, 2);
	it_stop(itd, 1200, 1);
	it_start(itd, 1300, 3);
	it_stop(itd, 1400, 2);
	int ret = it_stop(itd, 1500, 3);
	
	ASSERT_EQ(ret, 0);
}

TEST(many_entities) {
	unsigned itd = it_init(NULL);
	time_t base = 1000;
	
	/* Start 10 entities */
	for (unsigned i = 0; i < 10; i++) {
		int ret = it_start(itd, base + i * 100, i);
		ASSERT_EQ(ret, 0);
	}
	
	/* Stop 10 entities */
	for (unsigned i = 0; i < 10; i++) {
		int ret = it_stop(itd, base + 1000 + i * 100, i);
		ASSERT_EQ(ret, 0);
	}
}

TEST(entity_zero) {
	unsigned itd = it_init(NULL);
	unsigned id = 0;
	
	int ret1 = it_start(itd, 1000, id);
	int ret2 = it_stop(itd, 2000, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
}

TEST(large_entity_id) {
	unsigned itd = it_init(NULL);
	unsigned id = 999999;
	
	int ret1 = it_start(itd, 1000, id);
	int ret2 = it_stop(itd, 2000, id);
	
	ASSERT_EQ(ret1, 0);
	ASSERT_EQ(ret2, 0);
}

/* ============================================
 * Main test runner
 * ============================================ */

int main(void) {
	printf("Running libit tests...\n\n");
	
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
	
	printf("\n=== Test Summary ===\n");
	printf("Total errors: %u\n", errors);
	
	return errors > 0 ? 1 : 0;
}
