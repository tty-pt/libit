all := libit test test_extended

LDLIBS-libit := -lqmap -lqsys
LDLIBS-test := -lqmap -lqsys -lit
LDLIBS-test_extended := -lqmap -lqsys -lit

CFLAGS += -g
CFLAGS += -O3 -mpopcnt -mavx2 -mfma

include ../mk/include.mk

test: all
	./test.sh

bench: all
	LD_LIBRARY_PATH=lib:$(LD_LIBRARY_PATH) ./bin/test_extended 2>&1 | grep -E 'µs|PASS|FAIL|ALL|==='
