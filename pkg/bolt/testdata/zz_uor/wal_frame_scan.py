#!/usr/bin/env python3
"""Read-only scanner for a NornicDB journal (WAL) written by b6d8e762a.

It never writes, truncates or locks anything. Run it on a COPY of the data
directory, or on the live directory while the engine is stopped.

Usage:
    python3 -I wal_frame_scan.py <data-dir>/wal          # the journal directory
    python3 -I wal_frame_scan.py <file> [<file> ...]      # single journal files
Options:
    --crc       also verify the CRC32-C of every frame (pure Python, slow:
                roughly 2-4 s per 16 MiB)
    --quiet     print only the summary and the flagged frames
    --json      print one JSON object per frame instead of text

Frame layout (b6d8e762a, pkg/storage/wal_atomic_record.go:17-46, all integers
little-endian, every frame starts on an 8-byte boundary):
    magic   4 bytes  0x454C4157 (the bytes "WALE")
    version 1 byte   1, 2 or 3 (b6d8e762a writes 3; 1/2 can be older history)
    length  4 bytes  payload length N (b6d8e762a writes any N; readers refuse
                     N > 16,777,216)
    payload N bytes
    crc     4 bytes  CRC32-C (Castagnoli) of the payload
    trailer 8 bytes  0xDEADBEEFFEEDFACE (version >= 2 only)
    padding 0-7 zero bytes to the next multiple of 8 (version >= 2 only)
Payload of version 3 (pkg/storage/wal_entry_codec.go:24-44):
    [headerLen u32][JSON header {"seq","ts","op","data":null,"checksum","database"}]
    [dataLen u32][data bytes]
Payload of version 1/2: one JSON object with "data" in base64.
A file that does not start with "WALE" is the historical headerless format
(one JSON object per line); it is reported, not parsed.

Exit status:
    0  no frame over 16 MiB in the files the new engine reads
    1  frames over 16 MiB exist, and every one is an embedding audit record
       (op "update_embedding" with an empty header data field); the new
       engine's upgrade accepts these and replaces their data with an
       omission marker (pkg/storage/wal_oversized_audit.go:21-83)
    2  at least one frame over 16 MiB is NOT such an audit record; the new
       engine refuses to open the store for upgrade
    3  a damaged frame before the end of a file (bad magic, bad trailer,
       bad length, CRC mismatch with --crc); see the report
    4  usage error or unreadable path
A torn final frame (the file ends inside it) is reported but does not by
itself change the exit status: the upgrade drops a torn tail of wal.log.
"""

import json
import os
import re
import struct
import sys

MAGIC = b"WALE"
TRAILER = 0xDEADBEEFFEEDFACE
LIMIT = 16 * 1024 * 1024  # 16,777,216: walMaxEntrySize
SEGMENT_RE = re.compile(r"^seg-(\d+)-(\d+)\.wal$")

_CRC_TABLE = None


def _crc32c_table():
    global _CRC_TABLE
    if _CRC_TABLE is None:
        table = []
        for i in range(256):
            c = i
            for _ in range(8):
                c = (c >> 1) ^ 0x82F63B78 if c & 1 else c >> 1
            table.append(c)
        _CRC_TABLE = table
    return _CRC_TABLE


def crc32c_stream(f, length, chunk=1 << 20):
    table = _crc32c_table()
    crc = 0xFFFFFFFF
    remaining = length
    while remaining > 0:
        block = f.read(min(chunk, remaining))
        if not block:
            raise EOFError("file ends inside the payload")
        remaining -= len(block)
        for b in block:
            crc = table[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def align8(n):
    return (n + 7) & ~7


def describe_v3_payload(f, start, size):
    """Read only the JSON header of a version-3 payload. Returns a dict."""
    info = {}
    f.seek(start)
    raw = f.read(4)
    if len(raw) < 4 or size < 8:
        info["payload_error"] = "payload too short"
        return info
    header_len = struct.unpack("<I", raw)[0]
    if header_len > LIMIT - 8 or header_len + 8 > size:
        info["payload_error"] = "header length %d invalid for payload %d" % (header_len, size)
        return info
    header = f.read(header_len)
    raw = f.read(4)
    if len(header) < header_len or len(raw) < 4:
        info["payload_error"] = "file ends inside the payload header"
        return info
    data_len = struct.unpack("<I", raw)[0]
    info["data_len"] = data_len
    if header_len + 8 + data_len != size:
        info["payload_error"] = "data length %d does not match payload %d" % (data_len, size)
    try:
        h = json.loads(header.decode("utf-8"))
        info["seq"] = h.get("seq")
        info["op"] = h.get("op")
        info["database"] = h.get("database", "")
        info["ts"] = h.get("ts")
        info["header_data_empty"] = not h.get("data")
    except Exception as exc:  # noqa: BLE001 - report, never raise
        info["payload_error"] = "header JSON: %s" % exc
    return info


def scan_file(path, verify_crc, emit):
    """Scan one journal file. Returns a summary dict."""
    summary = {"path": path, "frames": 0, "bytes": 0, "over_limit": [],
               "over_limit_audit": 0, "over_limit_other": 0,
               "damaged": None, "torn_tail": None, "format": "framed",
               "largest": 0}
    size = os.path.getsize(path)
    summary["bytes"] = size
    with open(path, "rb") as f:
        head = f.read(4)
        if size == 0:
            summary["format"] = "empty"
            return summary
        if head != MAGIC[:len(head)] or (len(head) == 4 and head != MAGIC):
            summary["format"] = "headerless-json (historical, not parsed)"
            return summary
        offset = 0
        while offset < size:
            f.seek(offset)
            hdr = f.read(9)
            if len(hdr) < 9:
                if hdr == MAGIC[:len(hdr)] or hdr[:4] == MAGIC:
                    summary["torn_tail"] = {"offset": offset, "reason": "file ends inside a frame header"}
                else:
                    summary["damaged"] = {"offset": offset, "reason": "trailing bytes are not a frame"}
                break
            magic, version, length = hdr[:4], hdr[4], struct.unpack("<I", hdr[5:9])[0]
            if magic != MAGIC:
                summary["damaged"] = {"offset": offset, "reason": "bad magic %r" % magic}
                break
            frame = {"file": path, "offset": offset, "version": version, "length": length,
                     "over_limit": length > LIMIT}
            if version not in (1, 2, 3):
                frame["note"] = "version %d is not one b6d8e762a writes" % version
            body_end = offset + 9 + length + 4
            total = body_end - offset
            if version >= 2:
                total = align8(9 + length + 4 + 8)
            if offset + total > size:
                frame["torn"] = True
                summary["torn_tail"] = {"offset": offset, "reason": "file ends inside a frame of %d payload bytes" % length,
                                        "missing_bytes": offset + total - size}
                if version >= 3 and size - (offset + 9) >= 8:
                    frame.update(describe_v3_payload(f, offset + 9, length))
                emit(frame)
                break
            if version >= 3:
                frame.update(describe_v3_payload(f, offset + 9, length))
            f.seek(body_end - 4)
            stored_crc = struct.unpack("<I", f.read(4))[0]
            if version >= 2:
                trailer = struct.unpack("<Q", f.read(8))[0]
                if trailer != TRAILER:
                    frame["damaged"] = "bad trailer 0x%016x" % trailer
                pad = f.read(total - (9 + length + 4 + 8))
                if pad.strip(b"\x00"):
                    frame["damaged"] = "non-zero padding"
            if verify_crc and "damaged" not in frame:
                f.seek(offset + 9)
                if crc32c_stream(f, length) != stored_crc:
                    frame["damaged"] = "CRC32-C mismatch"
            if frame["over_limit"]:
                audit = (frame.get("op") == "update_embedding" and frame.get("header_data_empty")
                         and "payload_error" not in frame and version == 3)
                frame["new_engine"] = ("accepted: embedding audit, data replaced by an omission marker"
                                       if audit else "REFUSED: upgrade stops with 'entry size exceeds maximum'")
                summary["over_limit"].append(frame)
                if audit:
                    summary["over_limit_audit"] += 1
                else:
                    summary["over_limit_other"] += 1
            summary["largest"] = max(summary["largest"], length)
            summary["frames"] += 1
            emit(frame)
            if "damaged" in frame:
                summary["damaged"] = {"offset": offset, "reason": frame["damaged"]}
                break
            offset += total
    return summary


def journal_files(wal_dir):
    """The files the new engine reads in a journal directory, and the rest."""
    read, ignored = [], []
    segdir = os.path.join(wal_dir, "segments")
    listed = []
    manifest = os.path.join(segdir, "manifest.json")
    if os.path.isfile(manifest):
        try:
            with open(manifest, "r", encoding="utf-8") as fh:
                listed = [s.get("path", "") for s in (json.load(fh).get("segments") or [])]
        except Exception as exc:  # noqa: BLE001
            print("WARNING: cannot parse %s: %s" % (manifest, exc))
    seen = set()
    for name in listed:
        p = os.path.join(segdir, name)
        if os.path.isfile(p):
            read.append(p)
            seen.add(name)
    if os.path.isdir(segdir):
        # Unlisted seg-<first>-<last>.wal files: the engine reads an unlisted
        # newest tail; scanning all of them is the conservative superset.
        for name in sorted(os.listdir(segdir)):
            p = os.path.join(segdir, name)
            if name in seen or not os.path.isfile(p):
                continue
            if SEGMENT_RE.match(name):
                read.append(p)
            elif name != "manifest.json":
                ignored.append(p)
    active = os.path.join(wal_dir, "wal.log")
    if os.path.isfile(active):
        read.append(active)
    for name in sorted(os.listdir(wal_dir)):
        p = os.path.join(wal_dir, name)
        if name in ("wal.log", "segments"):
            continue
        ignored.append(p + ("/" if os.path.isdir(p) else ""))
    return read, ignored


def main(argv):
    args = [a for a in argv if not a.startswith("--")]
    flags = {a for a in argv if a.startswith("--")}
    unknown = flags - {"--crc", "--quiet", "--json"}
    if not args or unknown:
        print(__doc__)
        return 4
    verify_crc, quiet, as_json = "--crc" in flags, "--quiet" in flags, "--json" in flags

    def emit(frame):
        flagged = frame.get("over_limit") or frame.get("damaged") or frame.get("torn")
        if quiet and not flagged:
            return
        if as_json:
            print(json.dumps(frame, sort_keys=True))
            return
        mark = ""
        if frame.get("over_limit"):
            mark = "  <== OVER 16 MiB: " + frame.get("new_engine", "")
        if frame.get("damaged"):
            mark += "  <== DAMAGED: " + frame["damaged"]
        if frame.get("torn"):
            mark += "  <== TORN (file ends inside this frame)"
        print("%s offset=%d version=%d length=%d seq=%s op=%s db=%s%s" % (
            os.path.basename(frame["file"]), frame["offset"], frame["version"], frame["length"],
            frame.get("seq"), frame.get("op"), frame.get("database", ""), mark))

    files = []
    for a in args:
        if os.path.isdir(a):
            read, ignored = journal_files(a)
            print("Journal directory %s" % a)
            print("  read by the engine (in order): %s" % (", ".join(os.path.relpath(p, a) for p in read) or "none"))
            print("  not read by the engine: %s" % (", ".join(os.path.relpath(p, a) for p in ignored) or "none"))
            files.extend(read)
        elif os.path.isfile(a):
            files.append(a)
        else:
            print("ERROR: no such file or directory: %s" % a)
            return 4
    worst = 0
    totals = {"frames": 0, "over": 0, "audit": 0, "other": 0}
    for path in files:
        s = scan_file(path, verify_crc, emit)
        totals["frames"] += s["frames"]
        totals["over"] += len(s["over_limit"])
        totals["audit"] += s["over_limit_audit"]
        totals["other"] += s["over_limit_other"]
        print("SUMMARY %s: format=%s bytes=%d frames=%d largest_payload=%d over_16MiB=%d (audit=%d other=%d) torn_tail=%s damaged=%s" % (
            path, s["format"], s["bytes"], s["frames"], s["largest"], len(s["over_limit"]),
            s["over_limit_audit"], s["over_limit_other"],
            json.dumps(s["torn_tail"]) if s["torn_tail"] else "none",
            json.dumps(s["damaged"]) if s["damaged"] else "none"))
        if s["damaged"]:
            worst = max(worst, 3)
        if s["over_limit_other"]:
            worst = max(worst, 2)
        elif s["over_limit_audit"]:
            worst = max(worst, 1)
    verdict = {0: "OK: no frame over 16 MiB",
               1: "OVER-LIMIT AUDIT FRAMES ONLY: the upgrade accepts them (their vectors are dropped from the journal copy only)",
               2: "STOP: a frame over 16 MiB that is not an embedding audit; the upgrade will refuse",
               3: "STOP: a damaged frame before the end of a file"}[worst]
    print("TOTAL files=%d frames=%d over_16MiB=%d audit=%d other=%d -> exit %d: %s" % (
        len(files), totals["frames"], totals["over"], totals["audit"], totals["other"], worst, verdict))
    return worst


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
