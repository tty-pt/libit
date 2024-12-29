PREFIX ?= /usr/local
LIBDIR := $(DESTDIR)${PREFIX}/lib

npm-lib := @tty-pt/qhash
npm-root != npm root
npm-root-dir != dirname ${npm-root}
pwd != pwd
libdir := /usr/local/lib ${pwd} ${npm-lib:%=${npm-root}/%} \
	  ${npm-lib:%=${npm-root-dir}/../../%}
CFLAGS += ${libdir:%=-I%/include}
LDFLAGS	+= -lqhash -ldb ${libdir:%=-L%} ${libdir:%=-Wl,-rpath,%}

libit.so: libit.c include/it.h
	${CC} -o $@ libit.c -O3 -g -fPIC -shared -I/usr/local/include ${CFLAGS} ${LDFLAGS}

install: libit.so
	install -d ${DESTDIR}${PREFIX}/lib/pkgconfig
	install -m 644 libit.so ${DESTDIR}${PREFIX}/lib
	install -m 644 it.pc $(DESTDIR)${PREFIX}/lib/pkgconfig
	install -d ${DESTDIR}${PREFIX}/include
	install -m 644 include/it.h $(DESTDIR)${PREFIX}/include

.PHONY: install
