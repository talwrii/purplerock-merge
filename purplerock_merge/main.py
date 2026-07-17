#!/usr/bin/env python3
"""
purplerock-merge — content-addressed version history + two-way merge for note
vaults, without git.

The pieces, bottom up:

  record   Record a version of every changed *.md in a vault. A version is
           content -> sha256 -> stored under that hash, plus a line in an
           append-only log pointing at the file's previous version (parent).

  select   Resolve a selector to a set of notes. So far: children(NAME) ==
           notes whose inline `up:: [[NAME]]` field points at NAME.

  merge    Reconcile a note that lives in two vaults. Because the projector
           copies exact bytes, a note shared by both vaults shares hashes in
           both logs -- so the most recent hash common to both histories is
           the three-way merge base, for free. Then:
             heads equal              -> nothing to do
             one head is the other's  -> fast-forward that direction
               ancestor
             neither                  -> diverged: three-way merge from base
           A note in only one vault is projected into the other.

A vault's store lives at  <store-root>/<basename-of-vault>  -- outside the
vault, server-side, never synced back to devices.

Store layout (per note, mirroring the vault tree; blobs shared + deduped):
    <store-root>/<vault>/history/<relpath>   the note's version DAG, as
                                             blank-line-separated key:value
                                             records (parent: repeats for a
                                             merge, absent for a root)
    <store-root>/<vault>/objects/<sha256>    the bytes at each version

A merge records a node with two parents, so history is a real DAG, not a
chain -- ancestry is a parent-walk.

Deletes are never propagated (v1): adds and edits sync; a vanished note is
left alone. Losing a note to a sync bug is unacceptable; a leftover is not.

Runtime deps: record/select/log need none; merge shells out to `diff3`
(diffutils) for the three-way text merge, with a marker fallback if it is
absent; watch needs `inotify_simple`.
"""

import argparse
import hashlib
import json
import os
import re
import select
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone


def die(msg):
    print(f"purplerock-merge: {msg}", file=sys.stderr)
    sys.exit(1)


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def short(h):
    return h[:12] if h else "-"


def entry_parents(e):
    return e.get("parents") or []


def parse_records(text):
    """Parse a per-note history file: `key: value` lines, records blank-line
    separated. `parent:` may repeat (0 = root, 1 = edit, 2 = merge), so no
    braces, quotes, commas, or list syntax are ever needed."""
    records = []
    for block in text.split("\n\n"):
        rec = {"parents": []}
        for line in block.splitlines():
            key, sep, val = line.partition(":")
            if not sep:
                continue
            key, val = key.strip(), val.strip()
            if key == "parent":
                if val:
                    rec["parents"].append(val)
            else:
                rec[key] = val
        if "hash" in rec:
            records.append(rec)
    return records


def format_record(digest, parents, when):
    lines = [f"hash: {digest}"]
    lines += [f"parent: {p}" for p in parents]
    lines.append(f"time: {when}")
    return "\n".join(lines) + "\n\n"


# --- store -----------------------------------------------------------------

class Store:
    """One vault's version store: a per-note log tree + a shared blob pool.

        <root>/history/<relpath>   that note's version DAG (named like the note)
        <root>/objects/<sha256>    the bytes, deduped across all notes

    Each history file is `key: value` records, blank-line separated; `parent:`
    repeats for a merge (two parents) and is absent for a root.

    A note's history is a linear chain (each vault is a single sequential
    writer); branching happens only *across* vaults and is reconciled by the
    content hashes two chains share.
    """

    def __init__(self, root):
        self.root = os.path.abspath(root)
        self.objects = os.path.join(self.root, "objects")
        self.history_dir = os.path.join(self.root, "history")
        self._cache = {}       # path -> [hashes], oldest first (lazy per note)

    @classmethod
    def for_vault(cls, store_root, vault):
        return cls(os.path.join(store_root, os.path.basename(os.path.abspath(vault))))

    def ensure(self):
        os.makedirs(self.objects, exist_ok=True)
        os.makedirs(self.history_dir, exist_ok=True)

    # --- blobs (shared) ---

    def object_path(self, digest):
        return os.path.join(self.objects, digest)

    def read_object(self, digest):
        with open(self.object_path(digest), "rb") as f:
            return f.read()

    def write_object(self, digest, data):
        if os.path.exists(self.object_path(digest)):
            return
        tmp = self.object_path(digest) + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, self.object_path(digest))

    # --- per-note history ---

    def log_path(self, path):
        return os.path.join(self.history_dir, path)   # named exactly like the note

    def note_entries(self, path):
        """Version records for one note, oldest first."""
        p = self.log_path(path)
        if not os.path.exists(p):
            return []
        with open(p, encoding="utf-8") as f:
            recs = parse_records(f.read())
        for r in recs:
            r["path"] = path
        return recs

    def all_entries(self):
        """Every version record across the vault (unordered)."""
        for dirpath, _, filenames in os.walk(self.history_dir):
            for fn in filenames:
                fp = os.path.join(dirpath, fn)
                rel = os.path.relpath(fp, self.history_dir)
                with open(fp, encoding="utf-8") as f:
                    for r in parse_records(f.read()):
                        r["path"] = rel
                        yield r

    def history(self, path):
        """Hashes recorded for `path`, oldest first."""
        if path not in self._cache:
            self._cache[path] = [e["hash"] for e in self.note_entries(path)]
        return self._cache[path]

    def head(self, path):
        chain = self.history(path)
        return chain[-1] if chain else None

    def parents_map(self, path):
        """hash -> set of its parent hashes, unioned over the note's log."""
        m = {}
        for e in self.note_entries(path):
            m.setdefault(e["hash"], set()).update(entry_parents(e))
        return m

    def times(self, path):
        """hash -> most recent time it was recorded (used to order bases)."""
        t = {}
        for e in self.note_entries(path):
            t[e["hash"]] = e["time"]
        return t

    def ancestor_hashes(self, path, head):
        """Every hash reachable from `head` via parent edges, including head."""
        parents = self.parents_map(path)
        seen, stack = set(), [head]
        while stack:
            h = stack.pop()
            if h in seen:
                continue
            seen.add(h)
            stack.extend(parents.get(h, ()))
        return seen

    def append(self, path, digest, parents):
        p = self.log_path(path)
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        with open(p, "a", encoding="utf-8") as f:
            f.write(format_record(digest, parents, now_iso()))
        if path in self._cache:          # keep a warm cache in step
            self._cache[path].append(digest)


def record_note(store, vault, relpath, data=None, parents=None):
    """Record `relpath`'s current content as a new version if it changed.

    `parents` defaults to the single previous head (an ordinary edit); merge
    passes both merged heads. Returns the new hash, or None if the content
    already matches the head.
    """
    if data is None:
        with open(os.path.join(vault, relpath), "rb") as f:
            data = f.read()
    digest = sha256_bytes(data)
    if store.head(relpath) == digest:
        return None
    if parents is None:
        prev = store.head(relpath)
        parents = [prev] if prev else []
    store.write_object(digest, data)
    store.append(relpath, digest, parents)
    return digest


# --- scanning --------------------------------------------------------------

def iter_md_files(top):
    """Yield (relpath, abspath) for every *.md under top, skipping dotdirs."""
    top = os.path.abspath(top)
    for dirpath, dirnames, filenames in os.walk(top):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for name in filenames:
            if name.endswith(".md"):
                ab = os.path.join(dirpath, name)
                yield os.path.relpath(ab, top), ab


def md_paths(top):
    return {rel for rel, _ in iter_md_files(top)}


# --- selectors -------------------------------------------------------------
#
#     children(NAME)   notes whose `up` field links to [[NAME]]
#
# The hierarchy is an inline Dataview field at the top of a note:
#     up:: [[my possessions]]
# (a YAML frontmatter `up: [[..]]` is accepted too). A note may list several
# parents on one up line: `up:: [[a]], [[b]]`.

_UP_LINE = re.compile(r"^\s*up\s*::?\s*(.+?)\s*$", re.MULTILINE)
_WIKILINK = re.compile(r"\[\[([^\]|#]+)")


def up_targets(text):
    targets = set()
    for value in _UP_LINE.findall(text):
        for m in _WIKILINK.findall(value):
            targets.add(m.strip())
    return targets


def parse_selector(expr):
    m = re.match(r"^\s*(\w+)\((.*)\)\s*$", expr)
    if not m:
        die(f"cannot parse selector: {expr!r}")
    return m.group(1), m.group(2).strip()


def select(top, expr):
    fn, arg = parse_selector(expr)
    if fn != "children":
        die(f"unknown selector {fn!r} (only children() so far)")
    out = set()
    for rel, ab in iter_md_files(top):
        try:
            with open(ab, encoding="utf-8", errors="replace") as f:
                text = f.read()
        except OSError:
            continue
        if arg in up_targets(text):
            out.add(rel)
    return out


# --- three-way text merge --------------------------------------------------

def three_way(mine, base, theirs, label_mine="A", label_theirs="B"):
    """Merge two byte-strings against a common base.

    Returns (merged_bytes, conflict: bool). Uses `diff3 -m`; on conflict it
    embeds the usual <<<<<<< ======= >>>>>>> markers.
    """
    if mine == theirs:
        return mine, False
    with tempfile.TemporaryDirectory() as d:
        pm = os.path.join(d, "mine")
        pb = os.path.join(d, "base")
        pt = os.path.join(d, "theirs")
        for p, b in ((pm, mine), (pb, base), (pt, theirs)):
            with open(p, "wb") as f:
                f.write(b)
        try:
            r = subprocess.run(
                ["diff3", "-m", "-L", label_mine, "-L", "base", "-L", label_theirs,
                 pm, pb, pt],
                capture_output=True)
        except FileNotFoundError:
            # no diff3: don't guess, surface both sides behind markers
            merged = (b"<<<<<<< " + label_mine.encode() + b"\n" + mine +
                      b"=======\n" + theirs +
                      b">>>>>>> " + label_theirs.encode() + b"\n")
            return merged, True
        # diff3 -m: exit 0 = clean, 1 = conflict, >1 = trouble
        if r.returncode > 1:
            die(f"diff3 failed: {r.stderr.decode(errors='replace')}")
        return r.stdout, r.returncode == 1


# --- commands --------------------------------------------------------------

def cmd_record(args):
    vault = os.path.abspath(args.vault)
    if not os.path.isdir(vault):
        die(f"not a directory: {vault}")
    store = Store.for_vault(args.store_root, vault)
    store.ensure()
    scanned = changed = 0
    for rel, ab in iter_md_files(vault):
        scanned += 1
        try:
            with open(ab, "rb") as f:
                data = f.read()
        except OSError as e:
            print(f"skip {rel}: {e}", file=sys.stderr)
            continue
        digest = record_note(store, vault, rel, data)
        if digest is not None:
            changed += 1
            if args.verbose:
                print(f"record {rel} {short(digest)}")
    print(f"scanned {scanned} .md, recorded {changed} new version(s)")


def cmd_select(args):
    for rel in sorted(select(args.vault, args.selector)):
        print(rel)


def cmd_log(args):
    store = Store.for_vault(args.store_root, args.vault)
    if args.path is not None:
        entries = store.note_entries(args.path)               # already oldest-first
    else:
        entries = sorted(store.all_entries(), key=lambda e: e["time"])
    for e in reversed(entries):                                # newest first
        parents = ",".join(short(p) for p in entry_parents(e)) or "-"
        print(f"{e['time']}  {short(e['hash'])}  <-[{parents}]  {e['path']}")


def _atomic_write(path, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".prm-tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def cmd_merge(args):
    A = os.path.abspath(args.vault_a)
    B = os.path.abspath(args.vault_b)
    nameA, nameB = os.path.basename(A), os.path.basename(B)
    storeA = Store.for_vault(args.store_root, A)
    storeB = Store.for_vault(args.store_root, B)
    storeA.ensure()
    storeB.ensure()

    pathsA, pathsB = md_paths(A), md_paths(B)
    if args.selector:
        members = select(A, args.selector) | select(B, args.selector)
    else:
        members = pathsA | pathsB

    def note_action(msg):
        print(msg)

    for rel in sorted(members):
        inA, inB = rel in pathsA, rel in pathsB

        if inA and not inB:
            data = storeA.read_object(storeA.head(rel)) if storeA.head(rel) \
                else open(os.path.join(A, rel), "rb").read()
            if not args.dry_run:
                _atomic_write(os.path.join(B, rel), data)
                record_note(storeB, B, rel, data)
            note_action(f"project {nameA}->{nameB}  {rel}")
            continue
        if inB and not inA:
            data = storeB.read_object(storeB.head(rel)) if storeB.head(rel) \
                else open(os.path.join(B, rel), "rb").read()
            if not args.dry_run:
                _atomic_write(os.path.join(A, rel), data)
                record_note(storeA, A, rel, data)
            note_action(f"project {nameB}->{nameA}  {rel}")
            continue

        # present in both: make sure each side's current content is recorded,
        # so heads reflect on-disk reality before we compare.
        if not args.dry_run:
            record_note(storeA, A, rel)
            record_note(storeB, B, rel)
        headA, headB = storeA.head(rel), storeB.head(rel)
        if headA is None or headB is None:
            # no history yet (dry-run before any record) -- compare bytes
            da = open(os.path.join(A, rel), "rb").read()
            db = open(os.path.join(B, rel), "rb").read()
            if da != db:
                note_action(f"DIVERGE (no history) {rel}")
            continue

        if headA == headB:
            continue  # in sync

        ancA = storeA.ancestor_hashes(rel, headA)
        ancB = storeB.ancestor_hashes(rel, headB)
        if headB in ancA and headA not in ancB:
            # A descends from B's head -> A is ahead
            data = storeA.read_object(headA)
            if not args.dry_run:
                _atomic_write(os.path.join(B, rel), data)
                record_note(storeB, B, rel, data)
            note_action(f"ff {nameA}->{nameB}   {rel}  {short(headB)}->{short(headA)}")
        elif headA in ancB and headB not in ancA:
            data = storeB.read_object(headB)
            if not args.dry_run:
                _atomic_write(os.path.join(A, rel), data)
                record_note(storeA, A, rel, data)
            note_action(f"ff {nameB}->{nameA}   {rel}  {short(headA)}->{short(headB)}")
        else:
            # diverged: three-way from the most recent common ancestor
            common = ancA & ancB
            times = {**storeB.times(rel), **storeA.times(rel)}
            base = max(common, key=lambda h: times.get(h, "")) if common else None
            mine, theirs = storeA.read_object(headA), storeB.read_object(headB)
            base_bytes = b""
            if base is not None:
                base_bytes = (storeA.read_object(base)
                              if os.path.exists(storeA.object_path(base))
                              else storeB.read_object(base))
            merged, conflict = three_way(mine, base_bytes, theirs,
                                         label_mine=nameA, label_theirs=nameB)
            tag = "CONFLICT" if conflict else "merge"
            if not args.dry_run:
                # give each side the other's blob so both merge parents resolve
                storeA.write_object(headB, theirs)
                storeB.write_object(headA, mine)
                _atomic_write(os.path.join(A, rel), merged)
                _atomic_write(os.path.join(B, rel), merged)
                record_note(storeA, A, rel, merged, parents=[headA, headB])
                record_note(storeB, B, rel, merged, parents=[headA, headB])
            note_action(f"{tag} {rel}  base {short(base)}  ({nameA} {short(headA)} / {nameB} {short(headB)})")


# --- watch (inotify daemon) ------------------------------------------------
#
# Watches a vault with inotify and records a new version of a note once its
# edits settle. Debounce is per file: a note is committed after --quiet idle
# seconds, or forced after a --max-second burst so a long editing session still
# gets checkpointed. We record exact bytes, so this catches both local edits
# (CLOSE_WRITE) and files Syncthing lands via temp+rename (MOVED_TO).
#
# inotify is not recursive, so we add a watch per directory and, when a new
# directory appears (CREATE|ISDIR), watch it too.

def watch_relpath(vault, abspath):
    """relpath of a watched .md file, or None if it should be ignored."""
    if not abspath.endswith(".md"):
        return None
    rel = os.path.relpath(abspath, vault)
    if rel.startswith(".."):
        return None
    if any(part.startswith(".") for part in rel.split(os.sep)):
        return None  # skip .obsidian, .stfolder, .trash, dotfiles
    return rel


def flush_pending(store, vault, pending, verbose=True):
    recorded = 0
    for rel in sorted(pending):
        parent = store.head(rel)
        try:
            digest = record_note(store, vault, rel)
        except OSError:
            continue  # gone (moved/deleted) -- deletes are not propagated
        if digest is not None:
            recorded += 1
            if verbose:
                print(f"version {rel}  {short(digest)} <- {short(parent)}", flush=True)
    return recorded


def cmd_watch(args):
    try:
        from inotify_simple import INotify, flags
    except ImportError:
        die("watch needs inotify_simple (pip/pipx install inotify_simple)")

    vault = os.path.abspath(args.vault)
    if not os.path.isdir(vault):
        die(f"not a directory: {vault}")
    store = Store.for_vault(args.store_root, vault)
    store.ensure()

    inotify = INotify()
    watch_mask = flags.CLOSE_WRITE | flags.MOVED_TO | flags.CREATE
    wd_to_dir = {}

    def watch_tree(root):
        """Add a watch to `root` and every non-dotdir beneath it."""
        for dirpath, dirnames, _ in os.walk(root):
            dirnames[:] = [d for d in dirnames if not d.startswith(".")]
            wd = inotify.add_watch(dirpath, watch_mask)
            wd_to_dir[wd] = dirpath

    watch_tree(vault)

    # On startup, give a first version to any note that has no history yet, so
    # watch needs no prior `record` and nothing sits untracked. Notes that
    # already have history are left alone (their next edit is caught below).
    versioned = 0
    for rel, _ in iter_md_files(vault):
        if store.head(rel) is None:
            try:
                if record_note(store, vault, rel) is not None:
                    versioned += 1
            except OSError:
                continue
    if versioned:
        print(f"first version created for {versioned} untracked note(s)", flush=True)

    pending = set()
    first = last = None
    print(f"watching {vault}  ({len(wd_to_dir)} dirs, quiet={args.quiet}s, "
          f"max={args.max_secs}s) -- ctrl-c to stop", flush=True)
    try:
        while True:
            if pending:
                deadline = min(last + args.quiet, first + args.max_secs)
                timeout_ms = max(0.0, (deadline - time.monotonic()) * 1000)
            else:
                timeout_ms = None  # nothing pending: block until an event
            events = inotify.read(timeout=timeout_ms)
            if not events:
                if pending:
                    flush_pending(store, vault, pending)
                    pending.clear()
                    first = None
                continue
            for event in events:
                base = wd_to_dir.get(event.wd)
                if base is None:
                    continue
                full = os.path.join(base, event.name)
                if event.mask & flags.ISDIR:
                    if (event.mask & flags.CREATE) and not event.name.startswith("."):
                        watch_tree(full)  # a new folder: start watching it
                    continue
                rel = watch_relpath(vault, full)
                if rel is None:
                    continue
                pending.add(rel)
                last = time.monotonic()
                if first is None:
                    first = last
                if args.debug:
                    print(f"  changed {rel}", flush=True)
    except KeyboardInterrupt:
        if pending:
            flush_pending(store, vault, pending)
    finally:
        inotify.close()


# --- cli -------------------------------------------------------------------

def main(argv=None):
    p = argparse.ArgumentParser(prog="purplerock-merge", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("record", help="record a version of every changed .md")
    pr.add_argument("vault")
    pr.add_argument("--store-root", required=True)
    pr.add_argument("-v", "--verbose", action="store_true")
    pr.set_defaults(func=cmd_record)

    ps = sub.add_parser("select", help="list notes matched by a selector")
    ps.add_argument("vault")
    ps.add_argument("selector", help="e.g. 'children(my possessions)'")
    ps.set_defaults(func=cmd_select)

    pm = sub.add_parser("merge", help="reconcile two vaults")
    pm.add_argument("vault_a")
    pm.add_argument("vault_b")
    pm.add_argument("--store-root", required=True)
    pm.add_argument("--selector", help="limit to this set (evaluated on both)")
    pm.add_argument("--dry-run", action="store_true")
    pm.set_defaults(func=cmd_merge)

    pw = sub.add_parser("watch",
                        help="watch a vault and record versions as edits settle")
    pw.add_argument("vault")
    pw.add_argument("--store-root", required=True)
    pw.add_argument("--quiet", type=float, default=300.0,
                    help="record a note after this many idle seconds (default 300)")
    pw.add_argument("--max-secs", type=float, default=900.0, dest="max_secs",
                    help="force a record after a burst this long (default 900)")
    pw.add_argument("--debug", action="store_true")
    pw.set_defaults(func=cmd_watch)

    pl = sub.add_parser("log", help="show recorded history")
    pl.add_argument("vault")
    pl.add_argument("path", nargs="?")
    pl.add_argument("--store-root", required=True)
    pl.set_defaults(func=cmd_log)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
