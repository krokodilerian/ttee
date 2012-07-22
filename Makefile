CC=gcc -Wall -O3 -g -lpthread


TARGETS=ttee

all: $(TARGETS)

ttee: ttee.c config.h
	$(CC) -o ttee ttee.c

install: $(TARGETS)
	install $(TARGETS) /usr/local/bin/


clean:

	rm -f *~ $(TARGETS)




