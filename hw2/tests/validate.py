#!/usr/bin/env python3
"""Проверка целостности событий, очередей FIFO и инвариантов."""
import re
import sys
from collections import deque

rows = open(sys.argv[1], encoding="utf-8").read().splitlines()
previous_time = -1
queues = [deque() for _ in range(3)]
active = [None] * 3
buyers = {}   # buyer -> [position, route, phase]
seen_entry = set()
seen_exit = set()
starts = [0] * 3
ends = [0] * 3
closed = False
summary = {}

def match(pattern, text):
    m = re.fullmatch(pattern, text)
    return m.groups() if m else None

for row in rows:
    m = re.fullmatch(r'\[t=(\d+)\] (.+)', row)
    assert m, row
    time, event = int(m[1]), m[2]
    assert time >= previous_time, f"Время пошло назад: {row}"
    previous_time = time
    if event == 'Вход закрыт':
        assert not closed
        closed = True
    if vals := match(r'Вход: покупатель=(\d+)', event):
        who = int(vals[0]); assert who not in seen_entry and not closed
        seen_entry.add(who)
    if vals := match(r'План: покупатель=(\d+) отделы=([\d\->]+)', event):
        who = int(vals[0]); route = list(map(int, vals[1].split('->')))
        assert who in seen_entry and who not in buyers
        buyers[who] = [0, route, 'start']
    if vals := match(r'Переход: покупатель=(\d+) отдел=(\d+)', event):
        who, dep = map(int, vals)
        assert who in buyers and buyers[who][2] in ('start', 'next')
        assert buyers[who][1][buyers[who][0]] == dep
        buyers[who][2] = 'queue'
        queues[dep-1].append(who)
    if vals := match(r'Начало: покупатель=(\d+) отдел=(\d+) окончание=(\d+)', event):
        who, dep, finish = map(int, vals)
        dep -= 1
        assert active[dep] is None, f"Два покупателя у продавца {dep+1}"
        assert queues[dep].popleft() == who, f"Нарушен FIFO отдел {dep+1}"
        assert finish > time
        active[dep] = (who, finish)
        assert buyers[who][2] == 'queue'
        buyers[who][2] = 'service'
        starts[dep] += 1
    if vals := match(r'Конец: покупатель=(\d+) отдел=(\d+)', event):
        who, dep = map(int, vals)
        dep -= 1
        assert active[dep] == (who, time), f"Некорректное окончание в отделе {dep+1}"
        active[dep] = None
        assert buyers[who][2] == 'service'
        buyers[who][0] += 1
        buyers[who][2] = 'next' if buyers[who][0] < len(buyers[who][1]) else 'exit'
        ends[dep] += 1
    if vals := match(r'Выход: покупатель=(\d+) план=выполнен', event):
        who = int(vals[0]); assert buyers[who][2] == 'exit'
        buyers[who][2] = 'done'; seen_exit.add(who)
    if vals := match(r'Отказ: покупатель=(\d+) причина=вход_закрыт плановое_прибытие=(\d+)', event):
        assert closed and int(vals[0]) not in seen_entry
    if vals := match(r'Отдел: номер=(\d+) покупок=(\d+) очередь=(\d+) обслуживается=(\d+)', event):
        dep, purchases, qsize, serving = map(int, vals)
        dep -= 1
        assert purchases == ends[dep] and qsize == len(queues[dep])
        assert serving == (active[dep][0] if active[dep] else 0)
        assert starts[dep] >= ends[dep]
    if vals := match(r'Покупатели: всего=(\d+) вошли=(\d+) вышли=(\d+) внутри=(\d+) не_допущены=(\d+) ещё_не_пришли=(\d+)', event):
        summary = dict(zip(('total', 'entered', 'left', 'inside', 'rejected', 'future'), map(int, vals)))

assert summary, 'Нет итоговой статистики'
assert summary['entered'] == len(seen_entry)
assert summary['left'] == len(seen_exit)
assert summary['inside'] == summary['entered'] - summary['left']
assert summary['total'] == summary['entered'] + summary['rejected'] + summary['future']
if 'причина=завершено' in '\n'.join(rows):
    assert all(a is None for a in active)
    assert not any(queues)
    assert all(b[2] == 'done' for b in buyers.values())
    assert summary['inside'] == summary['future'] == 0
print(f"OK: покупателей={summary['total']}, обслуживаний={sum(ends)}")
