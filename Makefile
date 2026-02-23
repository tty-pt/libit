all := libit test

LDLIBS-libit := -lqmap -lqsys
LDLIBS-test := -lqmap -lqsys -lit

CFLAGS += -g

include ../mk/include.mk

test: all
	./test.sh
