CC = gcc
CFLAGS = -std=c11 -Wall -Wextra -Wpedantic -Wshadow -Wconversion -O2

all: server client observer

server: server.c
client: client.c
observer: client.c
	$(CC) $(CFLAGS) -DOBSERVER $< -o $@

test: all
	python3 tests/run.py

demo: all
	python3 tests/run.py --demo

clean:
	rm -f server client observer

.PHONY: all test demo clean
