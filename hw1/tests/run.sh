#!/usr/bin/env bash
set -eu

cd "$(dirname "$0")/.."
make -s shop
mkdir -p logs
work=$(mktemp -d logs/test-run.XXXXXX)
pid=""
reader_pid=""
passed=0

# Удаляет временные файлы и не оставляет фоновую программу при ошибке теста
cleanup()
{
    if [ -n "$pid" ]
    then
        kill -TERM "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
    if [ -n "$reader_pid" ]
    then
        kill -TERM "$reader_pid" 2>/dev/null || true
        wait "$reader_pid" 2>/dev/null || true
    fi
    rm -rf "$work"
}
trap cleanup EXIT

# Проверка инвариантов хранится в этом же скрипте
cat > "$work/check.awk" <<'AWK'
# Проверяет маршруты, очереди и занятость продавцов по журналу событий
function fail(message)
{
    print "Нарушение в строке " NR ": " message > "/dev/stderr"
    errors++
}

function value(key,    i, prefix)
{
    prefix = key "="
    for (i = 3; i <= NF; i++)
    {
        if (index($i, prefix) == 1)
        {
            return substr($i, length(prefix) + 1)
        }
    }
    return ""
}

{
    time = $1
    gsub(/[^0-9]/, "", time)
    time += 0
    if (time < previous_time)
    {
        fail("время идёт назад")
    }
    previous_time = time
    buyer = value("покупатель") + 0
    department = value("отдел") + 0
}

$2 == "Вход" \
{
    closed = 1
}

$2 == "Вход:" \
{
    if (closed || entered[buyer])
    {
        fail("повторный вход или вход после закрытия")
    }
    entered[buyer] = 1
    entered_count++
}

$2 == "План:" \
{
    count = split(value("отделы"), route, /->/)
    route_length[buyer] = count
    for (i = 1; i <= count; i++)
    {
        expected[buyer, i] = route[i]
    }
}

$2 == "Переход:" \
{
    if (!entered[buyer] || waiting[buyer] || active[buyer] || left[buyer])
    {
        fail("покупатель находится в нескольких местах")
    }
    if (department != expected[buyer, completed[buyer] + 1])
    {
        fail("нарушен маршрут")
    }
    waiting[buyer] = department
    tail[department]++
    queue[department, tail[department]] = buyer
}

$2 == "Начало:" \
{
    if (seller[department] || waiting[buyer] != department || active[buyer])
    {
        fail("продавец занят или покупатель не в нужной очереди")
    }
    head[department]++
    if (queue[department, head[department]] != buyer)
    {
        fail("нарушен FIFO")
    }
    if (visited[buyer, department])
    {
        fail("повторное посещение отдела")
    }
    if (value("окончание") + 0 <= time)
    {
        fail("неположительное время обслуживания")
    }
    finish_time[department] = value("окончание") + 0
    visited[buyer, department] = 1
    waiting[buyer] = 0
    seller[department] = buyer
    active[buyer] = department
}

$2 == "Конец:" \
{
    if (seller[department] != buyer || active[buyer] != department || finish_time[department] != time)
    {
        fail("некорректное окончание обслуживания")
    }
    seller[department] = 0
    active[buyer] = 0
    completed[buyer]++
    purchases[department]++
}

$2 == "Выход:" \
{
    if (!entered[buyer] || left[buyer] || waiting[buyer] || active[buyer] || completed[buyer] != route_length[buyer])
    {
        fail("выход до выполнения плана или повторный выход")
    }
    left[buyer] = 1
    left_count++
}

$2 == "Отказ:" \
{
    if (!closed || entered[buyer] || rejected[buyer])
    {
        fail("некорректный отказ")
    }
    rejected[buyer] = 1
    rejected_count++
}

$2 == "Итог:" \
{
    reason = value("причина")
    summary_count++
}

$2 == "Покупатели:" \
{
    if (value("вошли") + 0 != entered_count || value("вышли") + 0 != left_count || value("не_допущены") + 0 != rejected_count)
    {
        fail("статистика не соответствует событиям")
    }
    if (value("внутри") + 0 != entered_count - left_count)
    {
        fail("потерян покупатель")
    }
    if (value("всего") + 0 != entered_count + rejected_count + (value("ещё_не_пришли") + 0))
    {
        fail("нарушен баланс всех покупателей")
    }
    if (reason == "завершено" && (!closed || entered_count != left_count || value("ещё_не_пришли") + 0 != 0))
    {
        fail("преждевременное завершение")
    }
}

$2 == "Отдел:" \
{
    department = value("номер") + 0
    if (value("покупок") + 0 != purchases[department] || value("очередь") + 0 != tail[department] - head[department] || value("обслуживается") + 0 != seller[department])
    {
        fail("статистика отдела не соответствует событиям")
    }
}

END \
{
    if (summary_count != 1)
    {
        fail("нет единственного итога")
    }
    exit(errors != 0)
}
AWK

pass()
{
    passed=$((passed + 1))
    echo "PASS: $1"
}

# Проверяет также тождество вывода в терминал и журнала
run_ok()
{
    local name=$1
    timeout 10 ./shop "tests/$name.txt" "logs/$name.log" 0 1 > "$work/$name.out"
    cmp "$work/$name.out" "logs/$name.log"
    awk -f "$work/check.awk" "logs/$name.log"
    grep -Fq 'Итог: причина=завершено' "logs/$name.log"
    pass "$name: завершение, журнал и инварианты"
}

run_bad()
{
    local name=$1
    local result=0
    timeout 10 ./shop "tests/$name.txt" "$work/bad.log" 0 1 > "$work/bad.out" 2> "$work/bad.err" || result=$?
    test "$result" -eq 1
    test -s "$work/bad.err"
    pass "$name: ожидаемая ошибка"
}

for name in normal closing empty one route fifo parallel boundary closed random
do
    run_ok "$name"
done

grep -Fxq '[t=6] Итог: причина=завершено' logs/normal.log
grep -Fq 'Покупатели: всего=3 вошли=3 вышли=3 внутри=0 не_допущены=0' logs/normal.log
grep -Fq 'Отдел: номер=1 покупок=3' logs/normal.log
grep -Fq 'Отдел: номер=2 покупок=1' logs/normal.log
grep -Fq 'Отдел: номер=3 покупок=1' logs/normal.log
pass 'normal: точные время и статистика'

grep -Fxq '[t=8] Итог: причина=завершено' logs/closing.log
grep -Fq 'Покупатели: всего=3 вошли=2 вышли=2 внутри=0 не_допущены=1' logs/closing.log
pass 'closing: обслуживание после закрытия'

grep -Fxq '[t=5] Итог: причина=завершено' logs/empty.log
grep -Fxq '[t=6] Итог: причина=завершено' logs/route.log
grep -Fxq '[t=8] Итог: причина=завершено' logs/fifo.log
grep -Fxq '[t=3] Итог: причина=завершено' logs/parallel.log
grep -Fq 'вошли=1 вышли=1 внутри=0 не_допущены=1' logs/boundary.log
grep -Fxq '[t=0] Итог: причина=завершено' logs/closed.log
pass 'граничные сценарии: точные ожидаемые результаты'

timeout 10 ./shop tests/random.txt "$work/repeat.log" 0 1 > "$work/repeat.out"
cmp logs/random.log "$work/repeat.log"
pass 'seed: повторяемость случайного сценария'

for ((seed = 0; seed < 20; seed++))
do
    timeout 10 ./shop tests/random.txt "$work/seed_$seed.log" 0 "$seed" > "$work/seed_$seed.out"
    awk -f "$work/check.awk" "$work/seed_$seed.log"
    grep -Fq 'вошли=8 вышли=8 внутри=0' "$work/seed_$seed.log"
    grep -Fq 'Отдел: номер=1 покупок=6' "$work/seed_$seed.log"
    grep -Fq 'Отдел: номер=2 покупок=5' "$work/seed_$seed.log"
    grep -Fq 'Отдел: номер=3 покупок=6' "$work/seed_$seed.log"
done
pass '20 seed: маршруты, FIFO и баланс покупок'

timeout 10 ./shop tests/normal.txt "$work/delay.log" 1 1 > "$work/delay.out"
tail -n +2 logs/normal.log > "$work/without_delay.txt"
tail -n +2 "$work/delay.log" > "$work/with_delay.txt"
cmp "$work/without_delay.txt" "$work/with_delay.txt"
pass 'задержка не меняет модельные события'

for name in invalid_duplicate invalid_department invalid_range invalid_truncated invalid_extra invalid_zero_service invalid_negative invalid_overflow invalid_token
do
    run_bad "$name"
done

# Проверяет вместимость очереди на максимальном числе покупателей
{
    echo '100 0 1'
    echo '1 1'
    echo '1 1'
    echo '1 1'
    for ((i = 0; i < 100; i++))
    do
        echo '3 1 2 3'
    done
} > "$work/maximum.txt"
timeout 10 ./shop "$work/maximum.txt" logs/maximum.log 0 1 > "$work/maximum.out"
awk -f "$work/check.awk" logs/maximum.log
grep -Fxq '[t=102] Итог: причина=завершено' logs/maximum.log
grep -Fq 'вошли=100 вышли=100 внутри=0' logs/maximum.log
for department in 1 2 3
do
    grep -Fq "Отдел: номер=$department покупок=100 очередь=0 обслуживается=0" logs/maximum.log
done
pass '100 покупателей: все 300 покупок завершены'

cp tests/normal.txt "$work/input.txt"
cp "$work/input.txt" "$work/original.txt"
for alias in "$work/input.txt" "$work/hard.txt" "$work/symbolic.txt"
do
    if [ "$alias" = "$work/hard.txt" ]
    then
        ln "$work/input.txt" "$alias"
    elif [ "$alias" = "$work/symbolic.txt" ]
    then
        ln -s "$work/input.txt" "$alias"
    fi
    result=0
    timeout 10 ./shop "$work/input.txt" "$alias" 0 1 > "$work/alias.out" 2> "$work/alias.err" || result=$?
    test "$result" -eq 1
    cmp "$work/input.txt" "$work/original.txt"
done
pass 'защита входного файла и его ссылок от перезаписи'

result=0
timeout 10 ./shop tests/normal.txt "$work/missing/log.txt" 0 1 > "$work/missing.out" 2> "$work/missing.err" || result=$?
test "$result" -eq 1
pass 'ошибка открытия журнала'

result=0
timeout 10 ./shop "$work/absent.txt" "$work/absent.log" 0 1 > "$work/absent.out" 2> "$work/absent.err" || result=$?
test "$result" -eq 1
test -s "$work/absent.err"
pass 'отсутствующий входной файл'

result=0
timeout 10 ./shop tests "$work/directory.log" 0 1 > "$work/directory.out" 2> "$work/directory.err" || result=$?
test "$result" -eq 1
pass 'каталог вместо входного файла'

head -c 65536 /dev/zero > "$work/large.txt"
result=0
timeout 10 ./shop "$work/large.txt" "$work/large.log" 0 1 > "$work/large.out" 2> "$work/large.err" || result=$?
test "$result" -eq 1
pass 'слишком большой входной файл'

result=0
timeout 10 ./shop tests/normal.txt "$work/output.log" 0 1 > /dev/full 2> "$work/full.err" || result=$?
test "$result" -eq 1
grep -Fq 'причина=ошибка_ввода_вывода' "$work/output.log"
pass 'ошибка записи в стандартный вывод'

printf '0 0 5\n1 1\n1 1\n1 1\n\0' > "$work/nul.txt"
result=0
timeout 10 ./shop "$work/nul.txt" "$work/nul.log" 0 1 > "$work/nul.out" 2> "$work/nul.err" || result=$?
test "$result" -eq 1
pass 'нулевой байт во входном файле'

for arguments in '1001 1' '-1 1' '0 -1' '0 9999999999999999999999999'
do
    result=0
    read -r delay seed <<< "$arguments"
    timeout 10 ./shop tests/normal.txt "$work/args.log" "$delay" "$seed" > "$work/args.out" 2> "$work/args.err" || result=$?
    test "$result" -eq 1
done
pass 'проверка числовых аргументов'

./shop --help > "$work/help.txt"
grep -Fq 'Запуск:' "$work/help.txt"
result=0
./shop > "$work/noargs.out" 2> "$work/noargs.err" || result=$?
test "$result" -eq 1
pass 'справка и неверное число аргументов'

# Ждёт фактического начала обслуживания перед отправкой сигнала
for signal in INT TERM
do
    name="signal_$signal"
    rm -f "logs/$name.log"
    ./shop tests/interrupt.txt "logs/$name.log" 1000 1 > "$work/$name.out" &
    pid=$!
    ready=0
    for ((i = 0; i < 200; i++))
    do
        if [ -f "logs/$name.log" ] && grep -Fq 'Начало:' "logs/$name.log"
        then
            ready=1
            break
        fi
        sleep 0.01
    done
    test "$ready" -eq 1
    kill -"$signal" "$pid"
    for ((i = 0; i < 200; i++))
    do
        if ! kill -0 "$pid" 2>/dev/null
        then
            break
        fi
        sleep 0.01
    done
    if kill -0 "$pid" 2>/dev/null
    then
        kill -KILL "$pid"
        echo "FAIL: зависание после SIG$signal" >&2
        exit 1
    fi
    result=0
    wait "$pid" || result=$?
    pid=""
    if [ "$signal" = INT ]
    then
        test "$result" -eq 130
    else
        test "$result" -eq 143
    fi
    grep -Fq "Итог: причина=SIG$signal" "logs/$name.log"
    grep -Fq 'вошли=3 вышли=0 внутри=3' "logs/$name.log"
    awk -f "$work/check.awk" "logs/$name.log"
    pass "SIG$signal: быстрое завершение и сохранение состояния"
done

result=0
./shop --help > /dev/full 2> "$work/help_error.txt" || result=$?
test "$result" -eq 1
test -s "$work/help_error.txt"
pass 'справка: ошибка записи возвращает ненулевой код'

result=0
./shop tests/invalid_duplicate.txt "$work/duplicate.log" 0 1 > "$work/duplicate.out" 2> "$work/duplicate.err" || result=$?
test "$result" -eq 1
grep -Fq 'повтор отдела' "$work/duplicate.err"
grep -Fq 'Покупатель: 1' "$work/duplicate.err"
grep -Fq 'Пункт маршрута: 2' "$work/duplicate.err"
result=0
./shop tests/invalid_range.txt "$work/range.log" 0 1 > "$work/range.out" 2> "$work/range.err" || result=$?
test "$result" -eq 1
grep -Fq 'MAX' "$work/range.err"
grep -Fq 'Отдел: 1' "$work/range.err"
pass 'диагностика указывает параметр, отдел и место ошибки маршрута'

# Анонимный канал не требует поддержки специальных файлов на диске
for signal in INT TERM
do
    (
        trap - EXIT
        ./shop "$work/maximum.txt" "logs/blocked_$signal.log" 0 1 2> "$work/blocked_$signal.err" &
        writer=$!
        echo "$writer" > "$work/writer_$signal.pid"
        result=0
        wait "$writer" || result=$?
        echo "$result" > "$work/result_$signal.txt"
    ) | sleep 30 &
    reader_pid=$!
    for ((i = 0; i < 200; i++))
    do
        if [ -s "$work/writer_$signal.pid" ]
        then
            break
        fi
        sleep 0.01
    done
    pid=$(cat "$work/writer_$signal.pid")
    sleep 0.2
    kill -0 "$pid"
    kill -"$signal" "$pid"
    for ((i = 0; i < 200; i++))
    do
        if [ -s "$work/result_$signal.txt" ]
        then
            break
        fi
        sleep 0.01
    done
    if [ ! -s "$work/result_$signal.txt" ]
    then
        kill -KILL "$pid"
        echo "FAIL: зависание в заполненном канале после SIG$signal" >&2
        exit 1
    fi
    result=$(cat "$work/result_$signal.txt")
    pid=""
    if [ "$signal" = INT ]
    then
        test "$result" -eq 130
    else
        test "$result" -eq 143
    fi
    test ! -s "$work/blocked_$signal.err"
    grep -Fq "Итог: причина=SIG$signal" "logs/blocked_$signal.log"
    awk -f "$work/check.awk" "logs/blocked_$signal.log"
    kill -TERM "$reader_pid"
    wait "$reader_pid" 2>/dev/null || true
    reader_pid=""
    pass "SIG$signal: заполненный канал не мешает завершению"
done

# Проверяет обычную передачу всех событий через канал читающему процессу
set -o pipefail
timeout 10 ./shop tests/normal.txt "$work/pipe.log" 0 1 | cat > "$work/pipe.out"
cmp "$work/pipe.out" "$work/pipe.log"
pass 'обычный вывод через канал остаётся полным'

echo "Всего успешно: $passed"
