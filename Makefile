PREFIX ?= /usr/local

npm-lib := @tty-pt/qhash
npm-root != npm root
npm-root-dir != dirname ${npm-root}
pwd != pwd

prefix := ${pwd} ${npm-lib:%=${npm-root}/%} \
	${npm-lib:%=${npm-root-dir}/../../%} \
	/usr/local

CFLAGS := -O3 -g ${prefix:%=-I%/include} \
	-Wall -Wextra -Wpedantic

LDFLAGS	:= -lqhash -ldb ${prefix:%=-L%/lib} ${prefix:%=-Wl,-rpath,%/lib}

lib/libit.so: libit.c include/it.h lib
	${CC} -o $@ libit.c -fPIC -shared ${CFLAGS} ${LDFLAGS}

lib:
	mkdir $@ 2>/dev/null || true

install: lib/libit.so
	install -d ${DESTDIR}${PREFIX}/lib/pkgconfig
	install -m 644 lib/libit.so ${DESTDIR}${PREFIX}/lib
	install -m 644 it.pc $(DESTDIR)${PREFIX}/lib/pkgconfig
	install -d ${DESTDIR}${PREFIX}/include
	install -m 644 include/it.h $(DESTDIR)${PREFIX}/include

.PHONY: install
