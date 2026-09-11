all := libjoint test test_extended

LDLIBS-libjoint := -lqmap -lqsys
LDLIBS-test := -lqmap -lqsys -ljoint
LDLIBS-test_extended := -lqmap -lqsys -ljoint

CFLAGS += -g
CFLAGS += -O3 -mpopcnt -mavx2 -mfma

include ../mk/include.mk

test: all
	./test.sh

bench: all
	LD_LIBRARY_PATH=lib:$(LD_LIBRARY_PATH) ./bin/test_extended 2>&1 | grep -E 'µs|PASS|FAIL|ALL|==='
