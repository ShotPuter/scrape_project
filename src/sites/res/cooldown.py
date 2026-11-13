"""Interruptible worker cooldowns; a batch shares one durable SQLite deadline."""

from __future__ import annotations

import asyncio
import time

from src.utils.atomic import atomic_write_json


def state(args, runtime):
    tracker = getattr(args, "tracking_store", None)
    if tracker:
        return tracker.cooldown_state()
    result = dict(runtime.get("local_cooldown", {"cooldown_until": 0, "cycles": 0, "halted": 0}))
    result["active"] = result["cooldown_until"] > time.time() and not result["halted"]
    return result


def must_yield(args, runtime):
    control = state(args, runtime)
    return runtime.get("pause_requested", False) or control["active"] or control["halted"]


async def wait(args, runtime, report):
    tracker = getattr(args, "tracking_store", None)
    control = state(args, runtime)
    if runtime.pop("pause_requested", False) and not control["halted"]:
        seconds = args.error_cooldown_minutes * 60
        if tracker:
            control = tracker.request_cooldown(seconds, args.max_cooldowns)
        elif not control["active"]:
            cycles = control["cycles"]
            exhausted = cycles >= args.max_cooldowns
            control = {
                "cooldown_until": 0 if exhausted else time.time() + seconds,
                "cycles": cycles if exhausted else cycles + 1,
                "halted": int(exhausted),
                "active": not exhausted,
            }
            runtime["local_cooldown"] = control
    if control["halted"]:
        runtime["stop"] = True
        report["stop_reason"] = {"code": "cooldown_limit_reached", "count": control["cycles"], "limit": args.max_cooldowns}
        report["cooldown"] = control
        return
    entered = False
    while control["active"]:
        entered = True
        report["state"], report["cooldown"] = "cooldown", control
        atomic_write_json(args.output / f"report-{report['run_id']}.json", report)
        await asyncio.sleep(min(5.0, max(0.01, control["cooldown_until"] - time.time())))
        control = state(args, runtime)
    if control["halted"]:
        runtime["stop"] = True
        report["stop_reason"] = {"code": "cooldown_limit_reached", "count": control["cycles"], "limit": args.max_cooldowns}
        report["cooldown"] = control
        return
    if entered:
        if tracker:
            tracker.finish_cooldown()
        runtime["failures"] = 0
        report["state"], report["cooldown"] = "running", {**control, "active": False}
        atomic_write_json(args.output / f"report-{report['run_id']}.json", report)
