CC = gcc
CFLAGS = -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Wpedantic -O2

.PHONY: all clean test

all: shop
	mkdir -p logs

shop: main.c
	$(CC) $(CFLAGS) main.c -o shop

test: shop
	bash tests/run.sh

clean:
	rm -f shop
