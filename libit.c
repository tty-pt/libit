#define _DEFAULT_SOURCE
#include "./include/it.h"

#include <err.h>
#include <errno.h>
#include <limits.h>
#include <qhash.h>
#include <stdlib.h>
#include <string.h>

#ifdef __OpenBSD__
#include <db4/db.h>
#include <sys/queue.h>
#else
#include <db.h>
#include <bsd/sys/queue.h>
#endif

#ifdef __OpenBSD__
#define TS_MIN LLONG_MIN
#define TS_MAX LLONG_MAX
#else
#define TS_MIN LONG_MIN
#define TS_MAX LONG_MAX
#endif

#define TI_DBS_MAX 512

enum cflags {
	IT_AHEAD = 1, // first element
	IT_HD = 2, // iterating inside split
};

struct ti {
	time_t min, max;
	unsigned who;
};

struct isplit {
	time_t ts;
	int max;
	unsigned who;
};

struct match {
	struct ti ti;
	STAILQ_ENTRY(match) entry;
};

STAILQ_HEAD(match_stailq, match);

struct split {
	time_t min;
	time_t max;
	struct idm_list idml;
	unsigned count;
	TAILQ_ENTRY(split) entry;
};

TAILQ_HEAD(split_tailq, split);

struct tidbs {
	DB *ti; // keys and values are struct ti
	DB *max; // secondary DB (BTREE) with interval max as key
	DB *id; // secondary DB (BTREE) with ids as primary key
} ti_dbs[TI_DBS_MAX];

const time_t mtinf = (time_t) TS_MIN; // minus infinite
const time_t tinf = (time_t) TS_MAX; // infinite

static unsigned ti_n = 0;
static int ti_first = 1;

static struct idm idm;

static DB_ENV *dbe = NULL;

/* get timestamp from ISO-8601 date string */
time_t sscantime(char *buf) {
	char *aux;
	struct tm tm;

	memset(&tm, 0, sizeof(tm));
	aux = strptime(buf, "%Y-%m-%dT%H:%M:%S", &tm);
	if (!aux && !strptime(buf, "%Y-%m-%d", &tm)) {
		char *endptr;
		unsigned long long int timestamp = strtoull(buf, &endptr, 10);
		if (errno == 0 && *endptr == '\0' && buf != endptr)
			return (time_t)timestamp;
		else
			err(EXIT_FAILURE, "Invalid date or timestamp");
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

/* create time interval BTREE keys from time interval HASH db*/
static int
map_tidb_timaxdb(DB *sec, const DBT *key, const DBT *data, DBT *result)
{
	memset(result, 0, sizeof(DBT));
	result->size = sizeof(time_t);
	result->data = &((struct ti *) data->data)->max;
	return 0;
}

/* create id BTREE keys from time interval HASH db */
static int
map_tidb_tiiddb(DB *sec, const DBT *key, const DBT *data, DBT *result)
{
	memset(result, 0, sizeof(DBT));
	result->size = sizeof(unsigned);
	result->data = &((struct ti *) data->data)->who;
	return 0;
}

/******
 * key ordering compare functions
 ******/

/* compare two time intervals (for sorting BST items) */
static int
#ifdef __APPLE__
timax_cmp(DB *sec, const DBT *a_r, const DBT *b_r, size_t *locp)
#else
timax_cmp(DB *sec, const DBT *a_r, const DBT *b_r)
#endif
{
	time_t	a = * (time_t *) a_r->data,
		b = * (time_t *) b_r->data;
	return b > a ? -1 : (a > b ? 1 : 0);
}

/* compare two person ids (for sorting BST items) */
static int
#ifdef __APPLE__
tiid_cmp(DB *sec, const DBT *a_r, const DBT *b_r, size_t *locp)
#else
tiid_cmp(DB *sec, const DBT *a_r, const DBT *b_r)
#endif
{
	unsigned a = * (unsigned *) a_r->data,
		 b = * (unsigned *) b_r->data;
	return b > a ? -1 : (a > b ? 1 : 0);
}

/******
 * Database initializers
 ******/

/* initialize ti dbs */
static int
tidbs_init(struct tidbs *dbs, char *fname)
{
	return db_create(&dbs->ti, dbe, 0)
		|| dbs->ti->open(dbs->ti, NULL, fname, "ti", DB_HASH, DB_CREATE, 0664)

		|| db_create(&dbs->max, dbe, 0)
		|| dbs->max->set_bt_compare(dbs->max, timax_cmp)
		|| dbs->max->set_flags(dbs->max, DB_DUP)
		|| dbs->max->open(dbs->max, NULL, fname, "max", DB_BTREE, DB_CREATE, 0664)
		|| dbs->ti->associate(dbs->ti, NULL, dbs->max, map_tidb_timaxdb, DB_CREATE | DB_IMMUTABLE_KEY)

		|| db_create(&dbs->id, dbe, 0)
		|| dbs->id->set_bt_compare(dbs->id, tiid_cmp)
		|| dbs->id->set_flags(dbs->id, DB_DUP)
		|| dbs->id->open(dbs->id, NULL, fname, "id", DB_BTREE, DB_CREATE, 0664)
		|| dbs->ti->associate(dbs->ti, NULL, dbs->id, map_tidb_tiiddb, DB_CREATE | DB_IMMUTABLE_KEY);
}

/******
 * ti (struct ti to struct ti primary db) related functions
 ******/
 
/* insert a time interval into an AVL */
static int
ti_insert(struct tidbs *dbs, unsigned id, time_t start, time_t end)
{
	struct ti ti = { .min = start, .max = end, .who = id };
	DBT key, data;

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = &ti;
	key.size = sizeof(ti);
	data.data = &ti;
	data.size = sizeof(ti);

	return dbs->ti->put(dbs->ti, NULL, &key, &data, 0);
}

/* finish the last found interval at the provided timestamp for a certain
 * person id
 */
static int
ti_finish_last(struct tidbs *dbs, unsigned id, time_t end)
{
	struct ti ti;
	DBT key, data;
	DBT pkey;
	DBC *cur;
	int dbflags = DB_SET;

	dbs->id->cursor(dbs->id, NULL, &cur, 0);

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = &id;
	key.size = sizeof(id);

	do {
		if (cur->c_get(cur, &key, &data, dbflags)) {
			cur->close(cur);
			return 1;
		}

		memcpy(&ti, data.data, sizeof(ti));
		dbflags = DB_NEXT;
	} while (ti.max != tinf);

	cur->del(cur, 0);
	cur->close(cur);
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	key.data = data.data = &ti;
	key.size = data.size = sizeof(ti);
	/* ti.who = id; */
	ti.max = end;
	data.data = &ti;
	data.size = sizeof(ti);
	return dbs->ti->put(dbs->ti, NULL, &key, &data, 0);
}

/* intersect an interval with an AVL of intervals */
static inline unsigned
ti_intersect(struct tidbs *dbs, struct match_stailq *matches, time_t min, time_t max)
{
	struct ti tmp;
	DBC *cur;
	DBT key, data;
	int ret = 0, dbflags = DB_SET_RANGE;

	STAILQ_INIT(matches);
	dbs->max->cursor(dbs->max, NULL, &cur, 0);

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = &min;
	key.size = sizeof(time_t);

	while (1) {
		int res = cur->c_get(cur, &key, &data, dbflags);

		if (res == DB_NOTFOUND)
			break;

		dbflags = DB_NEXT;
		memcpy(&tmp, data.data, sizeof(struct ti));

		if (tmp.max >= min && tmp.min < max) {
			// its a match
			struct match *match = (struct match *) malloc(sizeof(struct match));
			memcpy(&match->ti, &tmp, sizeof(tmp));
			STAILQ_INSERT_TAIL(matches, match, entry);
			ret++;
		}
	}

	cur->close(cur);
	return ret;
}

int
ti_present(struct tidbs *dbs, time_t when, unsigned who) {
	int ret = 0, dbflags = DB_SET_RANGE;
	struct ti tmp;
	DBC *cur;
	DBT key, data;

	dbs->max->cursor(dbs->max, NULL, &cur, 0);

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = &when;
	key.size = sizeof(time_t);

	while (1) {
		int res = cur->c_get(cur, &key, &data, dbflags);

		if (res == DB_NOTFOUND)
			break;

		if (res)
			err(1, "ti_present\n");

		dbflags = DB_NEXT;
		memcpy(&tmp, data.data, sizeof(struct ti));

		if (tmp.who == who && tmp.max > when && tmp.min <= when) {
			ret++;
			break;
		}
	}

	cur->close(cur);
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
	unsigned i = 0;

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
split_create(unsigned who_hd, time_t min, time_t max)
{
	struct split *split = (struct split *) malloc(sizeof(struct split));
	struct hash_cursor c;
	unsigned id, ign;

	split->min = min;
	split->max = max;
	split->idml = idml_init();
	split->count = 0;

	c = hash_iter(who_hd, NULL, 0);

	while (hash_next(&id, &ign, &c)) {
		idml_push(&split->idml, id);
		split->count++;
	}

	return split;
}

/* Creates splits from the intermediary isplit array */
static inline void
splits_create(
		unsigned who_hd,
		struct split_tailq *splits,
		struct isplit *isplits,
		size_t matches_l)
{
	int i;

	TAILQ_INIT(splits);

	hash_drop(who_hd);

	for (i = 0; i < matches_l * 2 - 1; i++) {
		struct isplit *isplit = isplits + i;
		struct isplit *isplit2 = isplits + i + 1;
		struct split *split;
		time_t n, m;

		if (isplit->max)
			uhash_del(who_hd, isplit->who);
		else
			uhash_put(who_hd, isplit->who, &isplit->who, sizeof(isplit->who));

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
splits_init(unsigned who_hd, struct split_tailq *splits, struct match_stailq *matches, unsigned matches_l)
{
	struct isplit *isplits;
	struct isplit *isplit;
	struct isplit *buf;
	int i = 0;

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
	unsigned who_hd = hash_init();
	struct match_stailq matches;
	unsigned matches_l = ti_intersect(dbs, &matches, min, max);
	matches_fix(&matches, min, max);
	splits_init(who_hd, splits, &matches, matches_l);
	matches_free(&matches);
	hash_close(who_hd);
}

/* Inserts a tail queue of splits within another, before the element provided
 */
static inline void
splits_concat_before(
		struct split_tailq *target,
		struct split_tailq *origin,
		struct split *before)
{
	struct split *split, *tmp;
	TAILQ_FOREACH_SAFE(split, origin, entry, tmp) {
		TAILQ_REMOVE(origin, split, entry);
		TAILQ_INSERT_BEFORE(before, split, entry);
	}
}

/* Fills the spaces between splits (or on empty splits) with splits from BST B,
 * in order to resolve the situation where none of the people are present for
 * periods of time within the billing period (the aforementined gaps).
 */
static inline void
splits_fill(struct tidbs *tidbs, struct split_tailq *splits, time_t min, time_t max)
{
	struct split *split, *tmp;
	struct split_tailq more_splits;
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
static void
splits_free(struct split_tailq *splits)
{
	struct split *split, *split_tmp;

	TAILQ_FOREACH_SAFE(split, splits, entry, split_tmp) {
		idml_drop(&split->idml);
		TAILQ_REMOVE(splits, split, entry);
		free(split);
	}
}

/******
 * functions that process a valid type of line
 ******/

static inline int
it_exists(unsigned itd, time_t ts, unsigned id)
{
	struct tidbs *tidbs = &ti_dbs[itd];
	struct ti tmp;
	DBC *cur;
	DBT key, data;
	int ret = 0;

	tidbs->max->cursor(tidbs->max, NULL, &cur, 0);

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = &ts;
	key.size = sizeof(time_t);

	int res = cur->c_get(cur, &key, &data, DB_SET_RANGE);

	if (res != DB_NOTFOUND) {
		memcpy(&tmp, data.data, sizeof(struct ti));
		ret = tmp.max > ts && tmp.min <= ts;
	}

	cur->close(cur);
	return ret;
}

int
it_stop(unsigned itd, time_t ts, unsigned id)
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
it_start(unsigned itd, time_t ts, unsigned id)
{
	struct tidbs *tidbs = &ti_dbs[itd];

	if (ti_present(tidbs, ts, id))
		return 1;

	ti_insert(tidbs, id, ts, tinf);
	return 0;
}

struct it_internal {
	unsigned itd;
	struct split_tailq splits;
	struct split *next;
};

it_cur_t it_iter(unsigned itd, time_t start, time_t end)
{
	struct it_internal *internal = malloc(sizeof(struct it_internal));
	struct tidbs *tidbs = &ti_dbs[itd];
	splits_get(&internal->splits, tidbs, start, end);
	splits_fill(tidbs, &internal->splits, start, end);
	internal->next = TAILQ_FIRST(&internal->splits);
	internal->itd = itd;
	return internal;
}

int it_next(time_t *min, time_t *max, unsigned *count, unsigned *who, it_cur_t *c) {
	struct it_internal *internal = *c;
	unsigned ignore;

	if (!internal->next)
		return 0;

	while ((*who = idml_pop(&internal->next->idml)) == (unsigned) -1) {
		internal->next = TAILQ_NEXT(internal->next, entry);
		if (!internal->next)
			return 0;
	}

	*min = internal->next->min;
	*max = internal->next->max;
	*count = internal->next->count;
	return 1;
}

unsigned it_init(char *fname) {
	struct tidbs *tidbs;
	unsigned id;

	if (ti_first) {
		idm = idm_init();
		ti_first = 0;
	}

	id = idm_new(&idm);
	tidbs = &ti_dbs[id];

	tidbs_init(tidbs, fname);

	return id;
}
