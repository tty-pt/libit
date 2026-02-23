#define _DEFAULT_SOURCE
#ifndef __OpenBSD__
#define _XOPEN_SOURCE
#endif
#include "../include/ttypt/it.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <ttypt/queue.h>
#include <ttypt/qmap.h>
#include <ttypt/idm.h>
#include <ttypt/qsys.h>

#ifdef __OpenBSD__
#define TS_MIN LLONG_MIN
#define TS_MAX LLONG_MAX
#else
#define TS_MIN LONG_MIN
#define TS_MAX LONG_MAX
#endif

#define TI_DBS_MAX 512
#define SPLITS_WHO_MASK 0xFF
#define TI_MASK 0x7FF

enum cflags {
	IT_AHEAD = 1, // first element
	IT_HD = 2, // iterating inside split
};

struct ti {
	time_t min, max;
	uint32_t who;
};

struct isplit {
	time_t ts;
	int max;
	uint32_t who;
};

struct match {
	struct ti ti;
	STAILQ_ENTRY(match) entry;
};

STAILQ_HEAD(match_stailq, match);

struct split {
	time_t min;
	time_t max;
	ids_t ids;
	unsigned count;
	TAILQ_ENTRY(split) entry;
};

TAILQ_HEAD(split_tailq, split);

struct tidbs {
	uint32_t ti; // keys and values are struct ti
	uint32_t max; // secondary DB (BTREE) with interval max as key
	uint32_t id; // secondary DB (BTREE) with ids as primary key
} ti_dbs[TI_DBS_MAX];

const time_t mtinf = (time_t) TS_MIN; // minus infinite
const time_t tinf = (time_t) TS_MAX; // infinite

static int ti_first = 1;

static idm_t idm;

uint32_t qm_ti, qm_time, qm_id;

/* get timestamp from ISO-8601 date string */
time_t sscantime(char *buf) {
	char *aux;
	struct tm tm;

	memset(&tm, 0, sizeof(tm));
	aux = strptime(buf, "%Y-%m-%dT%H:%M:%S", &tm);
	if (!aux && !strptime(buf, "%Y-%m-%d", &tm)) {
		char *endptr;
		unsigned long long int timestamp = strtoull(buf, &endptr, 10);
		CBUG(errno || *endptr != '\0' || buf == endptr, "Invalid date or timestamp");
		return (time_t)timestamp;
	}

	tm.tm_isdst = -1;
	return mktime(&tm);
}

/* get ISO-8601 date string from timestamp
 *
 * only use this for debug (memory leak), or free pointer
 */
void printtime(char buf[DATE_MAX_LEN], time_t ts) {
	struct tm tm;

	if (ts == mtinf)
		strcpy(buf, "-inf");

	if (ts == tinf)
		strcpy(buf, "inf");

	tm = *localtime(&ts);

	if (tm.tm_sec || tm.tm_min || tm.tm_hour)
		strftime(buf, DATE_MAX_LEN, "%FT%T", &tm);
	else
		strftime(buf, DATE_MAX_LEN, "%F", &tm);
}

/******
 * key ordering compare functions
 ******/

/* compare two time intervals (for sorting BST items) */
static int
timax_cmp(const void * const a_r,
		const void * const b_r,
		size_t len UNUSED)
{
	time_t	a = * (time_t *) a_r,
		b = * (time_t *) b_r;
	return b > a ? -1 : (a > b ? 1 : 0);
}

/* compare two person ids (for sorting BST items) */
static int
tiid_cmp(const void * const a_r,
		const void * const b_r,
		size_t len UNUSED)
{
	uint32_t a = * (uint32_t *) a_r,
		 b = * (uint32_t *) b_r;
	return b > a ? -1 : (a > b ? 1 : 0);
}

/******
 * Database initializers
 ******/

__attribute__((constructor))
static void
libit_init(void)
{
	qm_ti = qmap_reg(sizeof(struct ti));
	qm_time = qmap_reg(sizeof(time_t));
	qm_id = qmap_reg(sizeof(uint32_t));
	qmap_cmp_set(qm_id, tiid_cmp);
}

/* initialize ti dbs */
static void
tidbs_init(struct tidbs *dbs, char *fname)
{
	dbs->ti = qmap_open(fname, "ti", qm_ti, qm_ti, TI_MASK, 0);
	dbs->max = qmap_open(fname, "max", qm_time, qm_ti, TI_MASK, QM_SORTED);
	qmap_cmp_set(qm_time, timax_cmp);
	dbs->id = qmap_open(fname, "id", qm_id, qm_ti, TI_MASK, QM_SORTED);
	/* NOTE: We do NOT use qmap_assoc because QM_SORTED doesn't support duplicate keys properly.
	 * Instead, we manually maintain the secondary indexes in ti_insert and ti_finish_last. */
}

/******
 * ti (struct ti to struct ti primary db) related functions
 ******/
 
/* insert a time interval */
static void
ti_insert(struct tidbs *dbs, uint32_t id, time_t start, time_t end)
{
	struct ti ti = { .min = start, .max = end, .who = id };
	qmap_put(dbs->ti, &ti, &ti);
}

/* finish the last found interval at the provided timestamp for a certain
 * person id
 */
static void
ti_finish_last(struct tidbs *dbs, uint32_t id, time_t end)
{
	struct ti ti, old_ti;
	const void *key, *value;
	uint32_t c = qmap_iter(dbs->ti, NULL, 0);  // Iterate through ALL intervals
	int found = 0;

	while (qmap_next(&key, &value, c)) {
		memcpy(&ti, value, sizeof(ti));
		
		/* Find the open interval (max=tinf) for this entity */
		if (ti.who == id && ti.max == tinf) {
			memcpy(&old_ti, &ti, sizeof(ti));
			found = 1;
			qmap_fin(c);
			break;
		}
	}

	if (!found) {
		return;  /* No open interval found */
	}

	/* Delete old interval and insert updated one */
	qmap_del(dbs->ti, &old_ti);
	
	ti.max = end;
	qmap_put(dbs->ti, &ti, &ti);
}

/* intersect an interval with an AVL of intervals */
static inline unsigned
ti_intersect(struct tidbs *dbs, struct match_stailq *matches, time_t min, time_t max)
{
	struct ti tmp;
	const void *key, *value;
	int ret = 0;

	STAILQ_INIT(matches);
	uint32_t c = qmap_iter(dbs->ti, NULL, 0);  // Iterate through ALL intervals

	while (qmap_next(&key, &value, c)) {
		memcpy(&tmp, value, sizeof(struct ti));

		if (tmp.max >= min && tmp.min < max) {
			// its a match
			struct match *match = (struct match *) malloc(sizeof(struct match));
			memcpy(&match->ti, &tmp, sizeof(tmp));
			STAILQ_INSERT_TAIL(matches, match, entry);
			ret++;
		}
	}
	
	qmap_fin(c);
	return ret;
}

int
ti_present(struct tidbs *dbs, time_t when, uint32_t who) {
	int ret = 0;
	struct ti tmp;
	uint32_t c = qmap_iter(dbs->ti, NULL, 0);  // Iterate through ALL intervals
	const void *key, *value;
	
	while (qmap_next(&key, &value, c)) {
		memcpy(&tmp, value, sizeof(struct ti));
		
		if (tmp.who == who && tmp.max > when && tmp.min <= when) {
			ret++;
			break;
		}
	}
	
	qmap_fin(c);
	return ret;
}

/******
 * matches related functions
 ******/

/* makes all provided matches lie within the provided interval [min, max] */
static inline void
matches_fix(struct match_stailq *matches, time_t min, time_t max)
{
	struct match *match;

	STAILQ_FOREACH(match, matches, entry) {
		if (match->ti.min < min)
			match->ti.min = min;
		if (match->ti.max > max)
			match->ti.max = max;
	}
}

static void
matches_free(struct match_stailq *matches)
{
	struct match *match, *match_tmp;

	STAILQ_FOREACH_SAFE(match, matches, entry, match_tmp) {
		STAILQ_REMOVE_HEAD(matches, entry);
		free(match);
	}
}

/******
 * isplit related functions
 ******/

/* compares isplits, so that we can sort them */
static int
isplit_cmp(const void *ap, const void *bp)
{
	struct isplit a, b;
	memcpy(&a, ap, sizeof(struct isplit));
	memcpy(&b, bp, sizeof(struct isplit));
	if (b.ts > a.ts)
		return -1;
	if (a.ts > b.ts)
		return 1;
	if (b.max > a.max)
		return -1;
	if (a.max > b.max)
		return 1;
	return 0;
}

// assumes isplits is of size matches_l * 2
/* creates intermediary isplits */
static inline struct isplit *
isplits_create(struct match_stailq *matches, size_t matches_l) {
	struct isplit *isplits = (struct isplit *) malloc(sizeof(struct isplit) * matches_l * 2);
	struct match *match;
	uint32_t i = 0;

	STAILQ_FOREACH(match, matches, entry) {
		struct isplit *isplit = isplits + i * 2;
		isplit->ts = match->ti.min;
		isplit->max = 0;
		isplit->who = match->ti.who;
		isplit++;
		isplit->ts = match->ti.max;
		isplit->max = 1;
		isplit->who = match->ti.who;
		i ++;
	};

	return isplits;
}

/******
 * split related functions
 ******/

/* Creates one split from its interval, and the list of people that are present
 */
static inline struct split *
split_create(uint32_t who_hd, time_t min, time_t max)
{
	struct split *split = (struct split *) malloc(sizeof(struct split));
	uint32_t c;
	const void *key, *value;

	split->min = min;
	split->max = max;
	split->ids = ids_init();
	split->count = 0;

	c = qmap_iter(who_hd, NULL, 0);

	while (qmap_next(&key, &value, c)) {
		uint32_t who_id = * (uint32_t *) key;
		ids_push(&split->ids, who_id);
		split->count++;
	}
	
	qmap_fin(c);
	return split;
}

/* Creates splits from the intermediary isplit array */
static inline void
splits_create(
		uint32_t who_hd,
		struct split_tailq *splits,
		struct isplit *isplits,
		size_t matches_l)
{
	size_t i;

	TAILQ_INIT(splits);

	qmap_drop(who_hd);

	for (i = 0; i < matches_l * 2 - 1; i++) {
		struct isplit *isplit = isplits + i;
		struct isplit *isplit2 = isplits + i + 1;
		struct split *split;
		time_t n, m;

		if (isplit->max)
			qmap_del(who_hd, &isplit->who);
		else
			qmap_put(who_hd, &isplit->who, &isplit->who);

		n = isplit->ts;
		m = isplit2->ts;

		if (n == m)
			continue;

		split = split_create(who_hd, n, m);
		TAILQ_INSERT_TAIL(splits, split, entry);
	}
}

/* From a list of matched intervals, this creates the tail queue of splits
 */
static void
splits_init(uint32_t who_hd, struct split_tailq *splits, struct match_stailq *matches, uint32_t matches_l)
{
	struct isplit *isplits;

	isplits = isplits_create(matches, matches_l);
	qsort(isplits, matches_l * 2, sizeof(struct isplit), isplit_cmp);
	splits_create(who_hd, splits, isplits, matches_l);
	free(isplits);
}

/* Obtains a tail queue of splits from the intervals that intersect the query
 * interval [min, max]
 */
static void
splits_get(struct split_tailq *splits, struct tidbs *dbs, time_t min, time_t max)
{
	uint32_t who_hd = qmap_open(NULL, NULL, QM_HNDL, QM_HNDL, SPLITS_WHO_MASK, 0);
	struct match_stailq matches;
	uint32_t matches_l = ti_intersect(dbs, &matches, min, max);
	
	/* If no matches, initialize empty split queue and return */
	if (matches_l == 0) {
		TAILQ_INIT(splits);
		qmap_close(who_hd);
		return;
	}
	
	matches_fix(&matches, min, max);
	splits_init(who_hd, splits, &matches, matches_l);
	matches_free(&matches);
	qmap_close(who_hd);
}

/* Inserts a tail queue of splits within another, before the element provided
 */
static inline void
splits_concat_before(
		struct split_tailq *target UNUSED,
		struct split_tailq *origin,
		struct split *before UNUSED)
{
	struct split *split, *tmp;
	TAILQ_FOREACH_SAFE(split, origin, entry, tmp) {
		TAILQ_REMOVE(origin, split, entry);
		TAILQ_INSERT_BEFORE(before, split, entry);
	}
}

/* Fills the spaces between splits (or on empty splits)
 * with splits from BST B, in order to resolve the situation
 * where none of the people are present for periods of time
 * within the billing period (the aforementined gaps).
 */
static inline void
splits_fill(struct tidbs *tidbs, struct split_tailq *splits, time_t min, time_t max)
{
	struct split *split, *tmp;
	time_t last_max;

	split = TAILQ_FIRST(splits);
	if (!split) {
		splits_get(splits, tidbs, min, max);
		return;
	}

	last_max = min;

	if (split->min > last_max) {
		struct split_tailq more_splits;
		splits_get(&more_splits, tidbs, last_max, split->min);
		splits_concat_before(splits, &more_splits, split);
	}

	last_max = split->max;

	TAILQ_FOREACH_SAFE(split, splits, entry, tmp) {
		if (!split->count) {
			struct split_tailq more_splits;
			splits_get(&more_splits, tidbs, split->min, split->max);
			splits_concat_before(splits, &more_splits, split);
			TAILQ_REMOVE(splits, split, entry);
		}

		last_max = split->max;
	}

	if (max > last_max) {
		struct split_tailq more_splits;
		splits_get(&more_splits, tidbs, last_max, max);
		TAILQ_CONCAT(splits, &more_splits, entry);
	}
}

/* Frees a tail queue of splits */
static void UNUSED
splits_free(struct split_tailq *splits)
{
	struct split *split, *split_tmp;

	TAILQ_FOREACH_SAFE(split, splits, entry, split_tmp) {
		ids_drop(&split->ids);
		TAILQ_REMOVE(splits, split, entry);
		free(split);
	}
}

/******
 * functions that process a valid type of line
 ******/

static inline int UNUSED
it_exists(uint32_t itd, time_t ts, uint32_t id UNUSED)
{
	struct tidbs *tidbs = &ti_dbs[itd];
	struct ti tmp;
	int ret = 0;
	uint32_t c = qmap_iter(tidbs->ti, NULL, 0);  // Iterate through ALL intervals
	const void *key, *value;

	while (qmap_next(&key, &value, c)) {
		memcpy(&tmp, value, sizeof(struct ti));
		if (tmp.max > ts && tmp.min <= ts) {
			ret = 1;
			break;
		}
	}
	
	qmap_fin(c);
	return ret;
}

int
it_stop(uint32_t itd, time_t ts, uint32_t id)
{
	struct tidbs *tidbs = &ti_dbs[itd];

	if (!ti_present(tidbs, ts, id)) {
		ti_insert(tidbs, id, mtinf, ts);
		return 1;
	}

	ti_finish_last(tidbs, id, ts);
	return 0;
}

int
it_start(uint32_t itd, time_t ts, uint32_t id)
{
	struct tidbs *tidbs = &ti_dbs[itd];

	if (ti_present(tidbs, ts, id))
		return 1;

	ti_insert(tidbs, id, ts, tinf);
	return 0;
}

struct it_internal {
	uint32_t itd;
	struct split_tailq splits;
	struct split *next;
};

it_cur_t it_iter(uint32_t itd, time_t start, time_t end)
{
	struct it_internal *internal = malloc(sizeof(struct it_internal));
	struct tidbs *tidbs = &ti_dbs[itd];
	splits_get(&internal->splits, tidbs, start, end);
	splits_fill(tidbs, &internal->splits, start, end);
	internal->next = TAILQ_FIRST(&internal->splits);
	internal->itd = itd;
	return internal;
}

int it_next(time_t *min, time_t *max, uint32_t *count, uint32_t *who, it_cur_t *c) {
	struct it_internal *internal = *c;

	if (!internal->next)
		return 0;

	while ((*who = ids_pop(&internal->next->ids)) == (uint32_t) -1) {
		internal->next = TAILQ_NEXT(internal->next, entry);
		if (!internal->next)
			return 0;
	}

	*min = internal->next->min;
	*max = internal->next->max;
	*count = internal->next->count;
	return 1;
}

uint32_t it_init(char *fname) {
	struct tidbs *tidbs;
	uint32_t id;

	if (ti_first) {
		idm = idm_init();
		ti_first = 0;
	}

	id = idm_new(&idm);
	tidbs = &ti_dbs[id];

	tidbs_init(tidbs, fname);

	return id;
}
