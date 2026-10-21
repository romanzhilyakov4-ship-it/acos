#!/usr/bin/env bash
# Одна команда «make test» проверяет старые сценарии ИДЗ-1 и новую многопоточность.
set -euo pipefail
cd "$(dirname "$0")/.."
tmp="$(mktemp -d)"
pid=""
cleanup() {
    if [[ -n "$pid" ]]; then
        kill -TERM "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
    rm -rf "$tmp"
}
trap cleanup EXIT
passed=0
pass() { printf 'PASS: %s\n' "$1"; passed=$((passed + 1)); }

run_ok() {
    local name="$1" seed="${2:-42}"
    timeout 15s ./shop "tests/$name.txt" "$tmp/$name.log" 0 "$seed" > "$tmp/$name.out"
    cmp -s "$tmp/$name.log" "$tmp/$name.out"
    grep -Fq 'Итог: причина=завершено' "$tmp/$name.log"
    python3 tests/validate.py "$tmp/$name.log" > /dev/null
    pass "$name: FIFO, маршруты, статистика"
}
run_bad() {
    local name="$1"
    if timeout 15s ./shop "tests/$name.txt" "$tmp/invalid.log" 0 42 >"$tmp/invalid.out" 2>"$tmp/invalid.err"; then
        echo "FAIL: $name принят как корректный" >&2; exit 1
    fi
    grep -Fq 'Некорректные данные' "$tmp/invalid.err"
    pass "$name: ошибка входных данных"
}

# ВСЕ десять положительных сценариев из hw1, без правки исходных .txt.
for name in boundary closed closing empty fifo normal one parallel random route; do
    run_ok "$name"
done
# Все девять отрицательных сценариев из hw1.
for name in invalid_department invalid_duplicate invalid_extra invalid_negative \
            invalid_overflow invalid_range invalid_token invalid_truncated invalid_zero_service; do
    run_bad "$name"
done

# Проверяем ожидаемые итоги базовых моделей, независимо от порядка потоков.
grep -Fq 'всего=3 вошли=3 вышли=3 внутри=0' "$tmp/normal.log"
grep -Fq 'всего=3 вошли=2 вышли=2 внутри=0 не_допущены=1' "$tmp/closing.log"
grep -Fq 'всего=2 вошли=1 вышли=1 внутри=0 не_допущены=1' "$tmp/boundary.log"
grep -Fq 'всего=2 вошли=0 вышли=0 внутри=0 не_допущены=2' "$tmp/closed.log"
grep -Fq 'всего=8 вошли=8 вышли=8 внутри=0' "$tmp/random.log"
pass 'контрольные итоги модели'

# Воспроизводимость не означает одинакового порядка событий у разных потоков:
# проверяем инварианты для нескольких seeds, а не точное совпадение журналов.
for seed in $(seq 0 19); do
    timeout 15s ./shop tests/random.txt "$tmp/seed.log" 0 "$seed" > "$tmp/seed.out"
    cmp -s "$tmp/seed.log" "$tmp/seed.out"
    python3 tests/validate.py "$tmp/seed.log" > /dev/null
    grep -Fq 'всего=8 вошли=8 вышли=8 внутри=0' "$tmp/seed.log"
done
pass '20 seeds: инварианты и итог'

# Максимум покупателей, все три отдела; гонки и переполнение очередей.
python3 - "$tmp/stress.txt" <<'PY'
import sys
with open(sys.argv[1], 'w', encoding='utf-8') as file:
    file.write('100 0 1\n1 5\n1 5\n1 5\n')
    for i in range(100):
        file.write('3 ' + ' '.join(str((i+j) % 3 + 1) for j in range(3)) + '\n')
PY
for seed in 1 42 123; do
    timeout 20s ./shop "$tmp/stress.txt" "$tmp/stress.log" 0 "$seed" > "$tmp/stress.out"
    cmp -s "$tmp/stress.log" "$tmp/stress.out"
    python3 tests/validate.py "$tmp/stress.log" > /dev/null
    grep -Fq 'всего=100 вошли=100 вышли=100 внутри=0' "$tmp/stress.log"
    for dep in 1 2 3; do
        grep -Fq "Отдел: номер=$dep покупок=100 очередь=0 обслуживается=0" "$tmp/stress.log"
    done
done
pass '100 покупателей, 300 покупок, 3 seeds'

# Лог нельзя записывать поверх входного файла (в том числе через ссылки).
cp tests/normal.txt "$tmp/input.txt"
cp "$tmp/input.txt" "$tmp/original.txt"
ln "$tmp/input.txt" "$tmp/hard.txt"
ln -s "$tmp/input.txt" "$tmp/sym.txt"
for alias in "$tmp/input.txt" "$tmp/hard.txt" "$tmp/sym.txt"; do
    if ./shop "$tmp/input.txt" "$alias" 0 42 >/dev/null 2>&1; then
        echo 'FAIL: вход перезаписан журналом' >&2; exit 1
    fi
    cmp -s "$tmp/input.txt" "$tmp/original.txt"
done
pass 'защита исходного файла'

# Ошибки файлов и параметров не должны приводить к запуску моделирования.
if ./shop "$tmp/no-input" "$tmp/no.log" 0 42 >/dev/null 2>&1; then exit 1; fi
if ./shop tests "$tmp/dir.log" 0 42 >/dev/null 2>&1; then exit 1; fi
if ./shop tests/normal.txt "$tmp/missing/subdir.log" 0 42 >/dev/null 2>&1; then exit 1; fi
for args in '1001 1' '-1 1' '0 -1' '0 999999999999999999999'; do
    if ./shop tests/normal.txt "$tmp/badargs.log" $args >/dev/null 2>&1; then exit 1; fi
done
./shop --help > "$tmp/help.txt"
grep -Fq 'Запуск:' "$tmp/help.txt"
pass 'ошибки файлов, аргументов, --help'

# SIGINT / SIGTERM: дождаться реального начала обслуживания и прервать.
for sig in INT TERM; do
    ./shop tests/interrupt.txt "$tmp/signal.log" 1000 42 > "$tmp/signal.out" &
    pid=$!
    ready=0
    for _ in $(seq 1 200); do
        if [[ -f "$tmp/signal.log" ]] && grep -Fq 'Начало:' "$tmp/signal.log"; then
            ready=1; break
        fi
        sleep 0.01
    done
    [[ "$ready" == 1 ]] || { echo 'FAIL: не началось обслуживание' >&2; exit 1; }
    kill -"$sig" "$pid"
    rc=0
    wait "$pid" || rc=$?
    pid=""
    [[ "$sig" == INT && "$rc" -eq 130 || "$sig" == TERM && "$rc" -eq 143 ]] || {
        echo "FAIL: SIG$sig, код $rc" >&2; exit 1;
    }
    grep -Fq "причина=SIG$sig" "$tmp/signal.log"
    python3 tests/validate.py "$tmp/signal.log" > /dev/null
    pass "SIG$sig: корректная остановка"
done

printf 'УСПЕШНО: %s групп проверок.\n' "$passed"
