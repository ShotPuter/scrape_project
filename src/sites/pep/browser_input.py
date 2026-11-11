"""Bounded CDP pointer movement experiment; not a claim of human input."""

from __future__ import annotations

import asyncio
import math
import random


def bezier_path(start: tuple[float, float], end: tuple[float, float], *, steps: int,
                width: float, height: float, rng=random) -> list[tuple[float, float]]:
    if not 15 <= steps <= 25 or not all(math.isfinite(n) for n in (*start, *end, width, height)):
        raise ValueError("Pointer path needs finite coordinates and 15..25 steps")
    if width <= 1 or height <= 1:
        raise ValueError("Pointer path needs a positive viewport")
    if not (0 <= end[0] < width and 0 <= end[1] < height):
        raise ValueError("Pointer target is outside the viewport")
    distance = math.dist(start, end)
    bend = min(distance / 4, 80)
    p1 = (start[0] + (end[0] - start[0]) / 3 + rng.uniform(-bend, bend),
          start[1] + (end[1] - start[1]) / 3 + rng.uniform(-bend, bend))
    p2 = (start[0] + 2 * (end[0] - start[0]) / 3 + rng.uniform(-bend, bend),
          start[1] + 2 * (end[1] - start[1]) / 3 + rng.uniform(-bend, bend))
    path = []
    for index in range(1, steps + 1):
        # Ease-out samples reduce displacement near the destination.
        t = 1 - (1 - index / steps) ** 2
        x = (1 - t) ** 3 * start[0] + 3 * (1 - t) ** 2 * t * p1[0] + 3 * (1 - t) * t ** 2 * p2[0] + t ** 3 * end[0]
        y = (1 - t) ** 3 * start[1] + 3 * (1 - t) ** 2 * t * p1[1] + 3 * (1 - t) * t ** 2 * p2[1] + t ** 3 * end[1]
        path.append((min(max(x, 0), width - 1), min(max(y, 0), height - 1)))
    path[-1] = end
    return path


async def move_pointer(page, start: tuple[float, float], end: tuple[float, float], *, width: float, height: float) -> int:
    from nodriver import cdp

    steps = random.randint(15, 25)
    duration = random.uniform(0.6, 1.2)
    for x, y in bezier_path(start, end, steps=steps, width=width, height=height):
        await page.send(cdp.input_.dispatch_mouse_event("mouseMoved", x=x, y=y))
        await asyncio.sleep(duration / steps)
    return steps


async def click_pointer(page, x: float, y: float) -> None:
    from nodriver import cdp

    await page.send(cdp.input_.dispatch_mouse_event("mousePressed", x=x, y=y,
                                                  button=cdp.input_.MouseButton.LEFT, buttons=1, click_count=1))
    await asyncio.sleep(random.uniform(0.06, 0.15))
    await page.send(cdp.input_.dispatch_mouse_event("mouseReleased", x=x, y=y,
                                                  button=cdp.input_.MouseButton.LEFT, buttons=0, click_count=1))
