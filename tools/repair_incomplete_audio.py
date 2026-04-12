#!/usr/bin/env python3
"""
修复 exports/ipod_touch/audio 中疑似不完整音频：
- 判定规则：文件大小 < 阈值 或 时长 <= 阈值
- 仅重下候选文件，完整文件跳过
- 重下后保留原有元数据标签
- 批量清理歌词中的时间戳 [mm:ss.xx]
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import requests
from mutagen.mp4 import MP4


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="重下不完整音频并清理歌词时间戳（含进度显示）"
    )
    parser.add_argument(
        "--audio-dir",
        default="exports/ipod_touch/audio",
        help="目标音频目录（默认 exports/ipod_touch/audio）",
    )
    parser.add_argument(
        "--db-path",
        default="exports/ipod_touch/pipeline_state.sqlite3",
        help="状态数据库路径（默认 exports/ipod_touch/pipeline_state.sqlite3）",
    )
    parser.add_argument(
        "--api-base",
        default="http://127.0.0.1:3000",
        help="本地 API 地址",
    )
    parser.add_argument(
        "--cookie",
        default=os.getenv("NCM_COOKIE", ""),
        help="登录 Cookie（默认读取环境变量 NCM_COOKIE）",
    )
    parser.add_argument(
        "--level",
        default="exhigh",
        help="song/url/v1 音质档位（默认 exhigh）",
    )
    parser.add_argument(
        "--size-threshold-mb",
        type=float,
        default=2.0,
        help="小于该体积视为疑似不完整（默认 2.0 MB）",
    )
    parser.add_argument(
        "--duration-threshold-sec",
        type=float,
        default=35.0,
        help="小于等于该时长视为疑似试听（默认 35 秒）",
    )
    parser.add_argument(
        "--bitrate",
        default="256k",
        help="转码 AAC 码率（默认 256k）",
    )
    return parser.parse_args()


def load_song_map_from_db(db_path: Path) -> dict[Path, int]:
    if not db_path.exists():
        raise FileNotFoundError(f"数据库不存在: {db_path}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT output_path, song_id
        FROM tracks
        WHERE status='success' AND output_path IS NOT NULL
        ORDER BY updated_at DESC
        """
    ).fetchall()
    conn.close()

    mapping: dict[Path, int] = {}
    for output_path, song_id in rows:
        p = Path(output_path).resolve()
        # 同一路径仅保留最新 song_id
        if p not in mapping:
            mapping[p] = int(song_id)
    return mapping


def build_session(cookie: str) -> requests.Session:
    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": "repair-incomplete-audio/1.0",
            "Referer": "https://music.163.com/",
        }
    )
    if cookie:
        sess.headers["Cookie"] = cookie
    return sess


def api_get(session: requests.Session, api_base: str, path: str, params: dict[str, Any]) -> dict[str, Any]:
    resp = session.get(
        f"{api_base.rstrip('/')}/{path.lstrip('/')}",
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def pick_audio_url(
    session: requests.Session,
    api_base: str,
    song_id: int,
    level: str,
) -> dict[str, Any] | None:
    # 1) song/url/v1
    try:
        d = api_get(session, api_base, "song/url/v1", {"id": song_id, "level": level})
        rows = d.get("data")
        cand = rows[0] if isinstance(rows, list) and rows else d.get("data", {})
        if isinstance(cand, dict) and cand.get("url"):
            return cand
    except Exception:
        pass

    # 2) song/url
    try:
        d2 = api_get(session, api_base, "song/url", {"id": song_id})
        rows2 = d2.get("data")
        cand2 = rows2[0] if isinstance(rows2, list) and rows2 else None
        if isinstance(cand2, dict) and cand2.get("url"):
            return cand2
    except Exception:
        pass

    # 3) song/url/match（最后尝试）
    try:
        d3 = api_get(session, api_base, "song/url/match", {"id": song_id})
        rows3 = d3.get("data")
        cand3 = rows3[0] if isinstance(rows3, list) and rows3 else d3.get("data", {})
        if isinstance(cand3, dict) and cand3.get("url"):
            return cand3
    except Exception:
        pass

    return None


def download_file(session: requests.Session, url: str, target: Path) -> None:
    with session.get(url, stream=True, timeout=90) as resp:
        resp.raise_for_status()
        with target.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)


def ffmpeg_convert(src: Path, dst: Path, bitrate: str) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(src),
        "-vn",
        "-ar",
        "44100",
        "-c:a",
        "aac",
        "-b:a",
        bitrate,
        str(dst),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"ffmpeg rc={proc.returncode}")


TIMESTAMP_RE = re.compile(
    r"^\s*\[(?:\d{1,2}:)?\d{1,2}:\d{2}(?:[.:]\d{1,3})?\]\s*"
)


def strip_lyric_timestamps(path: Path) -> bool:
    try:
        audio = MP4(str(path))
    except Exception:
        return False
    lyr = audio.get("\xa9lyr")
    if not lyr:
        return False
    raw = lyr[0]
    lines = raw.splitlines()
    changed = False
    out = []
    for line in lines:
        new_line = TIMESTAMP_RE.sub("", line)
        if new_line != line:
            changed = True
        out.append(new_line)
    if changed:
        audio["\xa9lyr"] = ["\n".join(out)]
        audio.save()
    return changed


def main() -> int:
    args = parse_args()
    audio_dir = Path(args.audio_dir).resolve()
    db_path = Path(args.db_path).resolve()
    if not audio_dir.exists():
        raise SystemExit(f"音频目录不存在: {audio_dir}")
    if not args.cookie:
        raise SystemExit("缺少 cookie：请设置 NCM_COOKIE 或 --cookie")

    session = build_session(args.cookie)
    song_map = load_song_map_from_db(db_path)
    files = sorted(audio_dir.glob("*.m4a"))

    candidates: list[tuple[Path, int, float, dict[str, Any]]] = []
    skipped_complete = 0
    parse_errors = 0

    size_limit = int(args.size_threshold_mb * 1024 * 1024)

    print(f"[scan] start: total_files={len(files)}")
    for i, p in enumerate(files, start=1):
        try:
            audio = MP4(str(p))
            size = p.stat().st_size
            duration = float(getattr(audio.info, "length", 0.0) or 0.0)
            tags = dict(audio.tags or {})
        except Exception:
            parse_errors += 1
            continue

        if size < size_limit or duration <= args.duration_threshold_sec:
            candidates.append((p, size, duration, tags))
        else:
            skipped_complete += 1

        if i % 100 == 0 or i == len(files):
            print(f"[scan] {i}/{len(files)}")

    print(
        "[scan] done: "
        f"candidates={len(candidates)} "
        f"skipped_complete={skipped_complete} "
        f"parse_errors={parse_errors}"
    )

    updated = 0
    skipped_no_songid = 0
    failed = 0
    failed_detail: list[str] = []

    total = len(candidates)
    for idx, (path, old_size, old_dur, old_tags) in enumerate(candidates, start=1):
        song_id = song_map.get(path.resolve())
        print(
            f"[repair] {idx}/{total} file={path.name} "
            f"old_size={old_size/1024/1024:.2f}MB old_dur={old_dur:.1f}s"
        )
        if not song_id:
            skipped_no_songid += 1
            print("         -> skip: no song_id mapping in sqlite")
            continue

        info = pick_audio_url(session, args.api_base, song_id, args.level)
        if not info or not info.get("url"):
            failed += 1
            failed_detail.append(f"{path.name} sid={song_id} no playable url")
            print(f"         -> fail: sid={song_id} no playable url")
            continue

        try:
            with tempfile.TemporaryDirectory(prefix="repair-incomplete-") as td:
                tdp = Path(td)
                raw = tdp / f"{song_id}.bin"
                out = tdp / f"{song_id}.m4a"
                download_file(session, info["url"], raw)
                ffmpeg_convert(raw, out, args.bitrate)

                new_audio = MP4(str(out))
                # 保留原文件元数据（标题/专辑/歌词/封面等）
                for k, v in old_tags.items():
                    new_audio[k] = v
                new_audio.save()

                shutil.move(str(out), str(path))
                updated += 1

                # 新文件仍执行一次歌词时间戳清理
                strip_lyric_timestamps(path)

                fresh = MP4(str(path))
                new_size = path.stat().st_size
                new_dur = float(getattr(fresh.info, "length", 0.0) or 0.0)
                print(
                    f"         -> ok: new_size={new_size/1024/1024:.2f}MB "
                    f"new_dur={new_dur:.1f}s sid={song_id}"
                )
        except Exception as exc:
            failed += 1
            failed_detail.append(f"{path.name} sid={song_id} {exc}")
            print(f"         -> fail: sid={song_id} err={exc}")

    # 全量再清理一次歌词时间戳（满足你的要求）
    print("[lyrics] stripping [mm:ss] timestamps for all files...")
    lyric_cleaned = 0
    for i, p in enumerate(files, start=1):
        if strip_lyric_timestamps(p):
            lyric_cleaned += 1
        if i % 150 == 0 or i == len(files):
            print(f"[lyrics] {i}/{len(files)}")

    print("=== summary ===")
    print(f"total_files={len(files)}")
    print(f"candidates={len(candidates)}")
    print(f"updated={updated}")
    print(f"skipped_complete={skipped_complete}")
    print(f"skipped_no_songid={skipped_no_songid}")
    print(f"failed={failed}")
    print(f"parse_errors={parse_errors}")
    print(f"lyrics_cleaned_files={lyric_cleaned}")
    if failed_detail:
        print("--- failed details (top 20) ---")
        for line in failed_detail[:20]:
            print(line)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
