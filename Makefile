PREFIX ?= /usr/local
LIBDIR := $(DESTDIR)${PREFIX}/lib

libit.so: libit.c include/it.h
	${CC} -o $@ libit.c -O3 -g -fPIC -shared -I/usr/local/include

install: libit.so
	install -d ${DESTDIR}${PREFIX}/lib/pkgconfig
	install -m 644 libit.so ${DESTDIR}${PREFIX}/lib
	install -m 644 it.pc $(DESTDIR)${PREFIX}/lib/pkgconfig
	install -d ${DESTDIR}${PREFIX}/include
	install -m 644 include/it.h $(DESTDIR)${PREFIX}/include

.PHONY: install
