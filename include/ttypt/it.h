#ifndef IT_H
#define IT_H

#include <time.h>
#include <sys/types.h>
#define DATE_MAX_LEN 20

typedef void * it_cur_t;

/* initialize an it db */
unsigned it_init(char *fname);

/* start an interval */
int it_start(unsigned itd, time_t ts, unsigned id);

/* stop an interval */
int it_stop(unsigned itd, time_t ts, unsigned id);

/* start iterating inside an interval */
it_cur_t it_iter(unsigned itd, time_t start, time_t end);

/* get next value within the interval */
int it_next(time_t *min, time_t *max, unsigned *count, unsigned *who, it_cur_t *c);

/* string to time_t */
time_t sscantime(char *buf);

/* time_t to string */
void printtime(char buf[DATE_MAX_LEN], time_t ts);

#endif
