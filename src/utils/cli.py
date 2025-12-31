from __future__ import annotations

import shutil


def hr(char: str = "-", *, padding: int = 0, fallback: int = 120) -> str:
    """Return a horizontal rule sized to the current terminal width.

    - Uses terminal width when available.
    - Avoids wrapping by staying <= (columns - 1).
    - Falls back to `fallback` when width can't be detected (e.g., redirected output).

    `padding` lets callers reserve columns (e.g., for prefixes).
    """
    if not char:
        char = "-"

    cols = shutil.get_terminal_size(fallback=(fallback, 20)).columns
    width = cols - 1 - max(0, int(padding))
    if width <= 0:
        width = max(1, int(fallback))

    return char[0] * width
