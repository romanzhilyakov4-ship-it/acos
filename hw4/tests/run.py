#!/usr/bin/env python3
import random
import re
import signal
import socket
import subprocess
import tempfile
import time
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "server"
CLIENT = ROOT / "client"
OBSERVER = ROOT / "observer"

NORMAL = (ROOT / "tests" / "normal.txt").read_text(encoding="utf-8")
CLOSING = "3 2 3\n4 4\n4 4\n4 4\n1 1\n1 1\n1 1\n"
EMPTY = "0 0 5\n1 1\n1 1\n1 1\n"
CLOSED = "2 0 0\n1 1\n1 1\n1 1\n1 1\n1 2\n"
FIFO = "4 0 1\n2 2\n2 2\n2 2\n1 1\n1 1\n1 1\n1 1\n"
PARALLEL = "3 0 1\n3 3\n3 3\n3 3\n1 1\n1 2\n1 3\n"
RANDOM = "8 1 20\n1 4\n2 5\n1 3\n3 1 2 3\n3 3 2 1\n2 2 1\n2 1 3\n1 1\n3 2 3 1\n1 3\n2 3 2\n"
STRESS = "100 0 1\n1 3\n1 3\n1 3\n" + "3 1 2 3\n" * 100


def port_number():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def await_port(port, process, limit=4):
    until = time.monotonic() + limit
    while time.monotonic() < until:
        if process.poll() is not None:
            raise AssertionError("Сервер завершился до запуска клиентов")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.02)
    raise AssertionError("Сервер не начал слушать порт")


def launch_client(port, kind, number=None, capture=False):
    args = [str(OBSERVER), "127.0.0.1", str(port)] if kind == "watch" else [str(CLIENT), "127.0.0.1", str(port), kind]
    if number is not None:
        args.append(str(number))
    if not capture:
        return subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    output = tempfile.TemporaryFile()
    proc = subprocess.Popen(args, stdout=output, stderr=subprocess.DEVNULL)
    proc.output = output
    return proc


def captured(proc):
    assert wait_process(proc, 5) == 0
    proc.output.seek(0)
    return proc.output.read().decode("utf-8")


def actor_clients(port, n, skip=None):
    result = []
    for d in range(1, 4):
        if skip != ("seller", d):
            result.append(launch_client(port, "seller", d))
    for b in range(1, n + 1):
        if skip != ("buyer", b):
            result.append(launch_client(port, "buyer", b))
    return result


def wait_process(process, seconds=20):
    try:
        return process.wait(timeout=seconds)
    except subprocess.TimeoutExpired:
        raise AssertionError(f"Процесс {process.pid} завис")


def wait_for(path, needle, seconds=5):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if path.exists() and needle in path.read_text(encoding="utf-8", errors="replace"):
            return
        time.sleep(0.02)
    raise AssertionError(f"Не удалось дождаться события: {needle}")


def validate(text, data):
    tokens = list(map(int, data.split()))
    n, interval, closing = tokens[:3]
    limits = list(zip(tokens[3:9:2], tokens[4:9:2]))
    location = 9
    routes = []
    for _ in range(n):
        length = tokens[location]
        routes.append(tokens[location + 1:location + length + 1])
        location += 1 + length
    assert location == len(tokens)
    queue = {1: [], 2: [], 3: []}
    active = {1: None, 2: None, 3: None}
    completed = [0] * n
    entrants, exits, refusals = set(), set(), set()
    purchases = [0, 0, 0]
    closed = False
    times = []
    for line in text.splitlines():
        match = re.match(r"\[t=(\d+)\] (.+)", line)
        assert match, line
        tick, msg = int(match[1]), match[2]
        times.append(tick)
        if msg == "Вход закрыт":
            assert tick == closing and not closed
            closed = True
        elif msg.startswith("Вход: покупатель="):
            who = int(msg.split("=")[1])
            assert who not in entrants and tick == (who - 1) * interval and tick < closing
            entrants.add(who)
        elif msg.startswith("Отказ: покупатель="):
            who = int(re.search(r"покупатель=(\d+)", msg)[1])
            assert who not in entrants and who not in refusals and tick == closing
            refusals.add(who)
        elif msg.startswith("Переход:"):
            who, dep = map(int, re.search(r"покупатель=(\d+) отдел=(\d+)", msg).groups())
            assert who in entrants and who not in exits
            assert routes[who - 1][completed[who - 1]] == dep
            queue[dep].append(who)
        elif msg.startswith("Начало:"):
            who, dep, finish = map(int, re.search(r"покупатель=(\d+) отдел=(\d+) окончание=(\d+)", msg).groups())
            assert active[dep] is None and queue[dep].pop(0) == who and finish > tick
            assert limits[dep - 1][0] <= finish - tick <= limits[dep - 1][1]
            active[dep] = (who, finish)
        elif msg.startswith("Конец:"):
            who, dep = map(int, re.search(r"покупатель=(\d+) отдел=(\d+)", msg).groups())
            assert active[dep] == (who, tick)
            active[dep] = None
            purchases[dep - 1] += 1
            completed[who - 1] += 1
        elif msg.startswith("Выход:"):
            who = int(re.search(r"покупатель=(\d+)", msg)[1])
            assert who not in exits and completed[who - 1] == len(routes[who - 1])
            exits.add(who)
        elif msg.startswith("Отдел: номер="):
            dep, sales, length, busy = map(int, re.search(
                r"номер=(\d+) покупок=(\d+) очередь=(\d+) обслуживается=(\d+)", msg).groups())
            assert sales == purchases[dep - 1] and length == len(queue[dep])
            assert busy == (active[dep][0] if active[dep] else 0)
        elif msg.startswith("Покупатели: всего="):
            nums = list(map(int, re.findall(r"=(\d+)", msg)))
            assert nums == [n, len(entrants), len(exits), len(entrants) - len(exits), len(refusals), 0]
    assert times == sorted(times)
    assert len(entrants) + len(refusals) == n
    assert len(exits) == len(entrants)
    assert all(not q for q in queue.values())
    assert all(x is None for x in active.values())
    assert closed
    assert "Итог: причина=завершено" in text
    assert "Покупатели: всего=" in text
    return len(entrants), sum(purchases)


def scenario(name, data, delay=0, seed=42, watches=1, change_watch=False, reconnect=None, bind="127.0.0.1", trace=True):
    n = int(data.split()[0])
    with tempfile.TemporaryDirectory(prefix="hw4-test-") as folder:
        folder = Path(folder)
        inp, log, out = folder / "input.txt", folder / "log.txt", folder / "stdout.txt"
        inp.write_text(data, encoding="utf-8")
        port = port_number()
        clients = []
        raw = None
        watchers = []
        with open(out, "w", encoding="utf-8") as stream:
            args = [str(SERVER), bind, str(port), str(inp), str(log), str(delay), str(seed)]
            if trace:
                args.append("--trace")
            proc = subprocess.Popen(args, stdout=stream, stderr=subprocess.DEVNULL)
            try:
                await_port(port, proc)
                for _ in range(watches):
                    watchers.append(launch_client(port, "watch", capture=True))
                if reconnect:
                    raw = socket.create_connection(("127.0.0.1", port), timeout=3)
                    raw.sendall(b"HEL")
                    raw.sendall(f"LO {reconnect[0]} {reconnect[1]}\n".encode())
                    raw.settimeout(5)
                    raw_file = raw.makefile("rb")
                    assert raw_file.readline().startswith(b"WELCOME")
                clients = actor_clients(port, n, reconnect)
                if reconnect:
                    # Отключаем участника именно тогда, когда от него ждут ответ
                    while True:
                        message = raw_file.readline()
                        assert message, "Сервер не отправил задание"
                        if message.startswith(b"TASK "):
                            break
                    raw_file.close()
                    raw.close()
                    raw = None
                    clients.append(launch_client(port, reconnect[0], reconnect[1]))
                if change_watch:
                    wait_for(log, "Вход:")
                    old = watchers.pop()
                    old.send_signal(signal.SIGTERM)
                    wait_process(old)
                    old.output.close()
                    watchers.append(launch_client(port, "watch", capture=True))
                assert wait_process(proc, 25) == 0, f"{name}: сервер завершился с ошибкой"
                for client in clients:
                    assert wait_process(client, 5) == 0, f"{name}: клиент завершился с ошибкой"
                text = log.read_text(encoding="utf-8")
                assert out.read_text(encoding="utf-8") == (text if trace else "")
                validate(text, data)
                for watcher in watchers:
                    replay = "\n".join(line for line in captured(watcher).splitlines() if line.startswith("[t="))
                    assert replay + "\n" == text, f"{name}: наблюдатель потерял события"
            finally:
                if raw is not None:
                    raw.close()
                if proc.poll() is None:
                    proc.kill()
                proc.wait()
                for c in clients + watchers:
                    if c.poll() is None:
                        c.kill()
                    c.wait()
                    if hasattr(c, "output"):
                        c.output.close()


def signal_case(signum):
    with tempfile.TemporaryDirectory(prefix="hw4-signal-") as folder:
        folder = Path(folder)
        inp, log = folder / "input", folder / "log"
        inp.write_text(STRESS, encoding="utf-8")
        port = port_number()
        proc = subprocess.Popen([str(SERVER), "127.0.0.1", str(port), str(inp), str(log), "100", "25"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        clients = []
        try:
            await_port(port, proc)
            watcher = launch_client(port, "watch", capture=True)
            clients = actor_clients(port, 100) + [watcher]
            wait_for(log, "Вход:")
            proc.send_signal(signum)
            assert wait_process(proc, 6) == 128 + signum
            assert f"Итог: причина={signal.Signals(signum).name}" in log.read_text(encoding="utf-8")
            for c in clients:
                assert wait_process(c, 5) == 0
            text = log.read_text(encoding="utf-8")
            replay = "\n".join(line for line in captured(watcher).splitlines() if line.startswith("[t="))
            assert replay + "\n" == text
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait()
            for c in clients:
                if c.poll() is None:
                    c.kill()
                c.wait()
                if hasattr(c, "output"):
                    c.output.close()


def bad_inputs():
    for data in ["-1 0 1\n1 1\n1 1\n1 1\n", "1 0 2\n1 1\n1 1\n1 1\n2 1 1\n",
                 "1 0 2\n1 0\n1 1\n1 1\n1 1\n", "1 0 2\n1 1\n1 1\n1 1\n2 1\n",
                 "1 0 2\n1 1\n1 1\n1 1\n1 1 extra\n", "1 0 2\n1 1\n1 1\n1 1\n1 4\n",
                 "1 0 2\n1 1\n1 1\n1 1\n1 1\x00\n"]:
        with tempfile.TemporaryDirectory() as folder:
            inp, log = Path(folder) / "input", Path(folder) / "log"
            inp.write_bytes(data.encode())
            completed = subprocess.run([str(SERVER), "127.0.0.1", "39009", str(inp), str(log), "0", "1"],
                                       capture_output=True, timeout=3)
            assert completed.returncode == 1
    with tempfile.TemporaryDirectory() as folder:
        inp = Path(folder) / "input"
        inp.write_text(NORMAL, encoding="utf-8")
        before = inp.read_bytes()
        completed = subprocess.run([str(SERVER), "127.0.0.1", "39009", str(inp), str(inp), "0", "1"],
                                   capture_output=True, timeout=3)
        assert completed.returncode == 1 and inp.read_bytes() == before


def main():
    for name, data in [("normal", NORMAL), ("closing", CLOSING), ("empty", EMPTY),
                       ("closed", CLOSED), ("fifo", FIFO), ("parallel", PARALLEL), ("random", RANDOM),
                       ("stress", STRESS)]:
        scenario(name, data)
    for seed in range(5):
        scenario(f"random-{seed}", RANDOM, seed=seed)
    generator = random.Random(17)
    for iteration in range(3):
        data = ["100 0 1", "1 5", "2 5", "1 4"]
        for _ in range(100):
            length = generator.randint(1, 3)
            route = generator.sample([1, 2, 3], length)
            data.append(" ".join(map(str, [length, *route])))
        scenario(f"random-stress-{iteration}", "\n".join(data) + "\n", seed=iteration)
    scenario("two-observers", NORMAL, watches=2, trace=False)
    scenario("observer-reconnect", NORMAL, delay=250, change_watch=True)
    scenario("buyer-reconnect", NORMAL, reconnect=("buyer", 1))
    scenario("seller-reconnect", NORMAL, reconnect=("seller", 1))
    scenario("any-interface", PARALLEL, bind="0.0.0.0")
    signal_case(signal.SIGINT)
    signal_case(signal.SIGTERM)
    bad_inputs()


def demo():
    with tempfile.TemporaryDirectory(prefix="hw4-demo-") as folder:
        log = Path(folder) / "shop.log"
        port = port_number()
        server = subprocess.Popen([str(SERVER), "127.0.0.1", str(port),
                                   str(ROOT / "tests" / "normal.txt"), str(log), "150", "42"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        clients = []
        try:
            await_port(port, server)
            watcher = launch_client(port, "watch", capture=True)
            clients = actor_clients(port, 3)
            assert wait_process(server, 15) == 0
            for client in clients:
                assert wait_process(client) == 0
            print(captured(watcher), end="")
            watcher.output.close()
        finally:
            if server.poll() is None:
                server.kill()
            server.wait()
            for client in clients:
                if client.poll() is None:
                    client.kill()
                client.wait()


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--demo":
        demo()
    elif len(sys.argv) == 1:
        main()
    else:
        raise SystemExit("Запуск: python3 tests/run.py [--demo]")
