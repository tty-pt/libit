all := libit test test_extended

LDLIBS-libit := -lqmap -lqsys
LDLIBS-test := -lqmap -lqsys -lit
LDLIBS-test_extended := -lqmap -lqsys -lit

CFLAGS += -g

include ../mk/include.mk

test: all
	./test.sh
