CC = gcc
CFLAGS = -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Werror -O2
LDLIBS = -pthread -lrt
SHELL = /bin/bash
.ONESHELL:
.PHONY: all test clean

all: shop actor

shop: main.c
	$(CC) $(CFLAGS) $< -o $@ $(LDLIBS)

actor: main.c
	$(CC) $(CFLAGS) -DBUILD_WORKER $< -o $@ $(LDLIBS)

test: all
	@set -euo pipefail
	dir=$$(mktemp -d)
	trap 'rm -rf "$$dir"' EXIT
	printf '3 1 10\n1 2\n2 3\n1 3\n2 1 2\n3 2 3 1\n1 3\n' > "$$dir/normal"
	printf '0 0 0\n1 1\n1 1\n1 1\n' > "$$dir/empty"
	printf '2 2 2\n1 1\n1 1\n1 1\n1 1\n1 2\n' > "$$dir/closing"
	printf '1 0 5\n2 1\n1 1\n1 1\n1 1\n' > "$$dir/invalid"
	printf '100 0 1\n1 5\n1 5\n1 5\n' > "$$dir/stress"
	for ((i=0; i<100; i++)); do
	    printf '3 %d %d %d\n' "$$((i%3+1))" "$$(((i+1)%3+1))" "$$(((i+2)%3+1))" >> "$$dir/stress"
	done
	for mode in fork-named fork-unnamed shm mq; do
	    for test in normal empty closing stress; do
	        workers=()
	        if [[ "$$mode" == shm || "$$mode" == mq ]]; then
	            ./shop "$$mode" "$$dir/$$test" "$$dir/log" 0 42 > "$$dir/out" & server=$$!
	            count=$$(head -n 1 "$$dir/$$test"); count=$${count%% *}
	            for ((i=1; i<=3; i++)); do ./actor "$$mode" "$$server" seller "$$i" & workers+=($$!); done
	            for ((i=1; i<=count; i++)); do ./actor "$$mode" "$$server" buyer "$$i" & workers+=($$!); done
	            wait "$$server"
	            for pid in "$${workers[@]}"; do wait "$$pid"; done
	        else
	            ./shop "$$mode" "$$dir/$$test" "$$dir/log" 0 42 > "$$dir/out"
	        fi
	        cmp -s "$$dir/log" "$$dir/out"
	        grep -q 'Итог: причина=завершено' "$$dir/log"
	        if [[ "$$test" == normal ]]; then
	            grep -q 'всего=3 вошли=3 вышли=3 внутри=0' "$$dir/log"
	        fi
	        if [[ "$$test" == stress ]]; then
	            grep -q 'всего=100 вошли=100 вышли=100 внутри=0' "$$dir/log"
	            for d in 1 2 3; do grep -q "Отдел: номер=$$d покупок=100 очередь=0 обслуживается=0" "$$dir/log"; done
	        fi
	    done
	    if ./shop "$$mode" "$$dir/invalid" "$$dir/log" 0 42 > /dev/null 2>&1; then
	        echo "Ошибка проверки: $$mode"
	        exit 1
	    fi
	done

clean:
	rm -f shop actor
