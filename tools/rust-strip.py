#!/usr/bin/env python3
"""Strip comments from Rust source, preserving string/char/raw-string literals.

Development tool: used to verify that comment-rewrite passes change no code
(compare comment-stripped text of two revisions), and to produce the clean
slate for re-documentation.
"""
import sys


def strip_rust_comments(src: str) -> str:
    out = []
    i = 0
    n = len(src)
    while i < n:
        ch = src[i]
        nxt = src[i + 1] if i + 1 < n else ""

        # Line comments (// /// //!).
        if ch == "/" and nxt == "/":
            while i < n and src[i] != "\n":
                i += 1
            continue

        # Block comments, nested.
        if ch == "/" and nxt == "*":
            depth = 1
            i += 2
            while i < n and depth > 0:
                if src.startswith("/*", i):
                    depth += 1
                    i += 2
                elif src.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            continue

        # Raw strings r"…" / r#"…"# and byte-raw br"…".
        if (ch == "r" and i + 1 < n and src[i + 1] in "\"#") or (
            ch == "b" and i + 2 < n and src[i + 1] == "r" and src[i + 2] in "\"#"
        ):
            k = i + (2 if ch == "b" else 1)
            hashes = 0
            while k < n and src[k] == "#":
                hashes += 1
                k += 1
            terminator = '"' + "#" * hashes
            end = src.find(terminator, k)
            if end == -1:
                out.append(src[i:])
                break
            out.append(src[i : end + len(terminator)])
            i = end + len(terminator)
            continue

        # Byte strings b"…".
        if ch == "b" and i + 1 < n and src[i + 1] == '"':
            out.append("b")
            i += 1
            ch = '"'

        # Plain strings with escapes.
        if ch == '"':
            out.append(ch)
            i += 1
            while i < n:
                c = src[i]
                if c == "\\" and i + 1 < n:
                    out.append(src[i : i + 2])
                    i += 2
                    continue
                out.append(c)
                i += 1
                if c == '"':
                    break
            continue

        # Char literals; lifetimes fall through untouched.
        if ch == "'":
            if i + 1 < n and src[i + 1] == "\\":
                j = i + 2
                while j < n and src[j] != "'":
                    j += 1
                out.append(src[i : j + 1])
                i = j + 1
                continue
            if i + 2 < n and src[i + 2] == "'":
                out.append(src[i : i + 3])
                i += 3
                continue
            out.append(ch)
            i += 1
            continue

        out.append(ch)
        i += 1

    return "\n".join(_without_trailing_comments("".join(out)))


def _without_trailing_comments(text: str):
    for line in text.split("\n"):
        stripped = line.lstrip()
        if stripped.startswith("//"):
            continue
        yield _strip_trailing(line)


def _strip_trailing(line: str) -> str:
    i = 0
    n = len(line)
    while i < n:
        ch = line[i]
        if ch == '"':
            i += 1
            closed = False
            while i < n:
                if line[i] == "\\":
                    i += 2
                    continue
                if line[i] == '"':
                    i += 1
                    closed = True
                    break
                i += 1
            if not closed:
                break
            continue
        if ch == "'" and i + 2 < n and line[i + 2] == "'":
            i += 3
            continue
        if ch == "/" and i + 1 < n and line[i + 1] == "/":
            return line[:i].rstrip()
        i += 1
    return line.rstrip()


if __name__ == "__main__":
    sys.stdout.write(strip_rust_comments(sys.stdin.read()))
