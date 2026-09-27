"""Local durable persistence via an append-only write-ahead log.

On-disk line format (one record per line)::

    <crc32-hex8>:<compact-json>\\n

A record is considered committed only after the full line (including the
trailing newline) has been written, flushed and fsynced. On recovery each
line is checksum-validated; the first line with a bad/missing checksum and
everything after it are discarded, which removes torn partial writes left
by a crash mid-write. A valid prefix is thus always recovered exactly, and
an unfinished write is never treated as a committed observation.
"""
from __future__ import annotations

import json
import os
import zlib
from typing import Any

_SEP = b":"


def encode_record(record: dict[str, Any]) -> bytes:
    payload = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
    crc = f"{zlib.crc32(payload) & 0xFFFFFFFF:08x}".encode("ascii")
    return crc + _SEP + payload + b"\n"


def decode_record(line: bytes) -> dict[str, Any] | None:
    """Decode one physical line; ``None`` means corrupt/incomplete."""
    if not line.endswith(b"\n"):
        return None
    line = line[:-1]
    crc_part, sep, payload = line.partition(_SEP)
    if not sep or len(crc_part) != 8:
        return None
    try:
        expected = int(crc_part, 16)
    except ValueError:
        return None
    if (zlib.crc32(payload) & 0xFFFFFFFF) != expected:
        return None
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    return value


class WalWriter:
    """Append-only writer with durable (fsync) commits."""

    def __init__(self, path: str, fsync: bool = True) -> None:
        self.path = path
        self._fsync = fsync
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._fh = open(path, "ab", buffering=0)

    def append(self, record: dict[str, Any]) -> None:
        """Durably append one record or raise (caller treats as not committed)."""
        data = encode_record(record)
        try:
            self._fh.write(data)
            self._fh.flush()
            if self._fsync:
                os.fsync(self._fh.fileno())
        except Exception:
            # Nothing beyond this point is trustworthy; close so recovery
            # truncates the torn tail rather than appending over it.
            self.close()
            raise

    def close(self) -> None:
        fh = getattr(self, "_fh", None)
        if fh is not None and not fh.closed:
            try:
                fh.flush()
                if self._fsync:
                    os.fsync(fh.fileno())
            finally:
                fh.close()


def read_wal(path: str) -> list[dict[str, Any]]:
    """Recover intact records; truncate the file at the first corrupt line.

    Returns the recovered records in append order. If the file does not
    exist the result is an empty list.
    """
    if not os.path.exists(path):
        return []
    with open(path, "rb") as fh:
        raw = fh.read()

    lines = raw.splitlines(keepends=True)
    records: list[dict[str, Any]] = []
    valid_bytes = 0
    corrupt = False
    for line in lines:
        decoded = decode_record(line)
        if decoded is None:
            corrupt = True
            break
        records.append(decoded)
        valid_bytes += len(line)

    if corrupt and valid_bytes < len(raw):
        # Drop the torn tail (partial write / garbage) durably.
        with open(path, "r+b") as fh:
            fh.truncate(valid_bytes)
            fh.flush()
            os.fsync(fh.fileno())

    return records
