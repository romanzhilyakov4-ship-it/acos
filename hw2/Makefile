CC = gcc
CFLAGS = -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Wpedantic -Wshadow -Wconversion -O2 -pthread
LDFLAGS = -pthread

.PHONY: all clean test sanitize
all: shop
	mkdir -p logs

shop: main.c
	$(CC) $(CFLAGS) main.c -o $@ $(LDFLAGS)

test: shop
	bash tests/run.sh

sanitize:
	$(CC) $(CFLAGS) -g -O1 -fsanitize=address,undefined -fno-omit-frame-pointer main.c -o shop_san $(LDFLAGS) -fsanitize=address,undefined

clean:
	rm -f shop shop_san
