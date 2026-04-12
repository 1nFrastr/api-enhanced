#!/usr/bin/env python3
"""
iPod touch 自动化音频打包管线

功能:
1) 读取歌单 JSON(包含 tracks 数组与 id 字段)
2) 调用本地 NCM API 获取详情、歌词、播放链接
3) 下载音频并转码为 m4a(AAC)
4) 写入封面/歌词/元数据到单个 m4a 文件
5) 记录可观测性数据(JSONL 日志 + SQLite 状态库 + 汇总报告)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
try:
    import requests
except ImportError as exc:
    raise SystemExit(
        "缺少 requests，请先安装: pip install -r tools/requirements-ipod-touch.txt"
    ) from exc

try:
    from mutagen.mp4 import MP4, MP4Cover
except ImportError as exc:
    raise SystemExit(
        "缺少 mutagen，请先安装: pip install -r tools/requirements-ipod-touch.txt"
    ) from exc


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(value: str, limit: int = 160) -> str:
    value = re.sub(r"[\\/:*?\"<>|]+", "_", value).strip()
    if not value:
        value = "untitled"
    return value[:limit]


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


@dataclass
class PipelineConfig:
    playlist_json: Path
    output_dir: Path
    api_base: str
    cookie: Optional[str]
    level: str
    bitrate: str
    retries: int
    timeout: int
    report_only: bool
    music_import: bool
    music_playlist: str
    limit: int


class ObservablePipeline:
    def __init__(self, cfg: PipelineConfig) -> None:
        self.cfg = cfg
        self.run_id = uuid.uuid4().hex[:12]
        self.run_dir = cfg.output_dir / "runs" / self.run_id
        self.audio_dir = cfg.output_dir / "audio"
        self.temp_dir = cfg.output_dir / "temp"
        self.log_file = self.run_dir / "events.jsonl"
        self.report_file = self.run_dir / "report.json"
        self.db_path = cfg.output_dir / "pipeline_state.sqlite3"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "ipod-touch-pipeline/1.0",
                "Accept": "application/json",
                "Referer": "https://music.163.com/",
            }
        )
        if cfg.cookie:
            self.session.headers["Cookie"] = cfg.cookie
        self._prepare_dirs()
        self.conn = self._open_db()
        self._init_db()
        self._create_run()

    def _prepare_dirs(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.audio_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def _open_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
              run_id TEXT PRIMARY KEY,
              started_at TEXT NOT NULL,
              finished_at TEXT,
              playlist_path TEXT NOT NULL,
              api_base TEXT NOT NULL,
              level TEXT NOT NULL,
              total_tracks INTEGER DEFAULT 0,
              success_tracks INTEGER DEFAULT 0,
              failed_tracks INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS tracks (
              run_id TEXT NOT NULL,
              song_id INTEGER NOT NULL,
              name TEXT,
              artist TEXT,
              album TEXT,
              status TEXT NOT NULL,
              attempts INTEGER NOT NULL DEFAULT 0,
              duration_ms INTEGER,
              output_path TEXT,
              audio_url TEXT,
              audio_type TEXT,
              file_size INTEGER,
              last_error TEXT,
              started_at TEXT,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (run_id, song_id)
            );

            CREATE INDEX IF NOT EXISTS idx_tracks_run_status
              ON tracks(run_id, status);
            """
        )
        self.conn.commit()

    def _create_run(self) -> None:
        self.conn.execute(
            """
            INSERT INTO runs(run_id, started_at, playlist_path, api_base, level)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                self.run_id,
                utc_now(),
                str(self.cfg.playlist_json),
                self.cfg.api_base,
                self.cfg.level,
            ),
        )
        self.conn.commit()

    def log_event(self, severity: str, stage: str, message: str, **kwargs: Any) -> None:
        payload: dict[str, Any] = {
            "ts": utc_now(),
            "run_id": self.run_id,
            "level": severity.upper(),
            "stage": stage,
            "message": message,
        }
        for key, value in kwargs.items():
            if key in payload:
                payload[f"extra_{key}"] = value
            else:
                payload[key] = value
        line = json.dumps(payload, ensure_ascii=False)
        print(line)
        ensure_parent(self.log_file)
        with self.log_file.open("a", encoding="utf-8") as fp:
            fp.write(line + "\n")

    def _api_get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.cfg.api_base.rstrip('/')}/{endpoint.lstrip('/')}"
        last_error: Optional[Exception] = None
        for attempt in range(1, self.cfg.retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.cfg.timeout)
                resp.raise_for_status()
                data = resp.json()
                return data
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                self.log_event(
                    "WARN",
                    "http_retry",
                    "API 请求失败，准备重试",
                    endpoint=endpoint,
                    params=params,
                    attempt=attempt,
                    error=str(exc),
                )
                time.sleep(min(2**attempt, 8))
        raise RuntimeError(f"API 请求失败: {endpoint} {params} error={last_error}")

    def _download_file(self, url: str, target: Path) -> int:
        ensure_parent(target)
        with self.session.get(url, timeout=self.cfg.timeout, stream=True) as resp:
            resp.raise_for_status()
            total = 0
            with target.open("wb") as fp:
                for chunk in resp.iter_content(chunk_size=1024 * 64):
                    if not chunk:
                        continue
                    fp.write(chunk)
                    total += len(chunk)
        return total

    def _download_bytes(self, url: str) -> bytes:
        resp = self.session.get(url, timeout=self.cfg.timeout)
        resp.raise_for_status()
        return resp.content

    def _ffmpeg_convert_to_m4a(self, src: Path, dst: Path) -> None:
        ensure_parent(dst)
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
            self.cfg.bitrate,
            str(dst),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            raise RuntimeError(
                f"ffmpeg 转码失败: rc={proc.returncode}, stderr={proc.stderr.strip()}"
            )

    def _write_m4a_tags(
        self,
        file_path: Path,
        *,
        title: str,
        artist: str,
        album: str,
        album_artist: str,
        track_no: int,
        disc_no: int,
        year: str,
        lyrics: str,
        cover_data: Optional[bytes],
        cover_content_type: Optional[str],
    ) -> None:
        audio = MP4(str(file_path))
        audio["\xa9nam"] = [title]
        audio["\xa9ART"] = [artist]
        audio["aART"] = [album_artist]
        audio["\xa9alb"] = [album]
        if year:
            audio["\xa9day"] = [year]
        audio["trkn"] = [(max(track_no, 1), 0)]
        audio["disk"] = [(max(disc_no, 1), 0)]
        if lyrics:
            audio["\xa9lyr"] = [lyrics]
        if cover_data:
            fmt = MP4Cover.FORMAT_JPEG
            if cover_content_type and "png" in cover_content_type.lower():
                fmt = MP4Cover.FORMAT_PNG
            audio["covr"] = [MP4Cover(cover_data, imageformat=fmt)]
        audio.save()

    def _music_import(self, track_path: Path) -> None:
        safe_track_path = str(track_path).replace('"', '\\"')
        safe_playlist = self.cfg.music_playlist.replace('"', '\\"')
        script = f'''
        tell application "Music"
          activate
          set importedTrack to add POSIX file "{safe_track_path}"
          try
            set targetPlaylist to playlist "{safe_playlist}"
          on error
            set targetPlaylist to make new user playlist with properties {{name:"{safe_playlist}"}}
          end try
          duplicate importedTrack to targetPlaylist
        end tell
        '''
        proc = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"导入 Music.app 失败: {proc.stderr.strip()}")

    def _parse_playlist_tracks(self) -> list[dict[str, Any]]:
        content = json.loads(self.cfg.playlist_json.read_text(encoding="utf-8"))
        tracks = content.get("tracks", [])
        if not isinstance(tracks, list) or not tracks:
            raise ValueError("playlist JSON 缺少 tracks 列表或为空")
        return tracks

    def _upsert_track(self, song_id: int, **kwargs: Any) -> None:
        now = utc_now()
        self.conn.execute(
            """
            INSERT INTO tracks (
              run_id, song_id, name, artist, album, status, attempts, duration_ms,
              output_path, audio_url, audio_type, file_size, last_error, started_at, updated_at
            ) VALUES (
              :run_id, :song_id, :name, :artist, :album, :status, :attempts, :duration_ms,
              :output_path, :audio_url, :audio_type, :file_size, :last_error, :started_at, :updated_at
            )
            ON CONFLICT(run_id, song_id) DO UPDATE SET
              name=excluded.name,
              artist=excluded.artist,
              album=excluded.album,
              status=excluded.status,
              attempts=excluded.attempts,
              duration_ms=excluded.duration_ms,
              output_path=excluded.output_path,
              audio_url=excluded.audio_url,
              audio_type=excluded.audio_type,
              file_size=excluded.file_size,
              last_error=excluded.last_error,
              started_at=COALESCE(tracks.started_at, excluded.started_at),
              updated_at=excluded.updated_at
            """,
            {
                "run_id": self.run_id,
                "song_id": song_id,
                "name": kwargs.get("name"),
                "artist": kwargs.get("artist"),
                "album": kwargs.get("album"),
                "status": kwargs.get("status", "queued"),
                "attempts": kwargs.get("attempts", 0),
                "duration_ms": kwargs.get("duration_ms"),
                "output_path": kwargs.get("output_path"),
                "audio_url": kwargs.get("audio_url"),
                "audio_type": kwargs.get("audio_type"),
                "file_size": kwargs.get("file_size"),
                "last_error": kwargs.get("last_error"),
                "started_at": kwargs.get("started_at"),
                "updated_at": now,
            },
        )
        self.conn.commit()

    def _pick_audio(self, song_id: int) -> dict[str, Any]:
        params = {"id": song_id, "level": self.cfg.level}
        data = self._api_get("song/url/v1", params)
        rows = data.get("data")
        if isinstance(rows, list) and rows:
            candidate = rows[0]
        else:
            candidate = data.get("data", {})
        if not isinstance(candidate, dict):
            raise RuntimeError("song/url/v1 返回结构异常")
        if candidate.get("url"):
            return candidate

        # 降级到 song/url
        fallback = self._api_get("song/url", {"id": song_id})
        rows2 = fallback.get("data")
        candidate2 = rows2[0] if isinstance(rows2, list) and rows2 else None
        if isinstance(candidate2, dict) and candidate2.get("url"):
            return candidate2
        raise RuntimeError(
            f"无法获取音频 URL: song_id={song_id}, v1={candidate.get('code')}, fallback={candidate2}"
        )

    def _fetch_song_detail(self, song_id: int) -> dict[str, Any]:
        data = self._api_get("song/detail", {"ids": song_id})
        songs = data.get("songs")
        if not isinstance(songs, list) or not songs:
            raise RuntimeError(f"song/detail 无数据: song_id={song_id}")
        return songs[0]

    def _fetch_lyric(self, song_id: int) -> str:
        data = self._api_get("lyric", {"id": song_id})
        lrc = data.get("lrc", {}) if isinstance(data, dict) else {}
        text = ""
        if isinstance(lrc, dict):
            text = lrc.get("lyric") or ""
        if not text:
            data2 = self._api_get("lyric/new", {"id": song_id})
            yrc = data2.get("yrc", {}) if isinstance(data2, dict) else {}
            if isinstance(yrc, dict):
                text = yrc.get("lyric") or ""
        return text.strip()

    def process_song(self, song_id: int, basic_name: str = "") -> None:
        started_at = utc_now()
        self._upsert_track(song_id, status="processing", started_at=started_at, attempts=1)
        start_ts = time.monotonic()

        try:
            detail = self._fetch_song_detail(song_id)
            name = detail.get("name") or basic_name or str(song_id)
            artists_data = detail.get("ar") or []
            artists = ", ".join(
                [x.get("name", "") for x in artists_data if isinstance(x, dict) and x.get("name")]
            ).strip() or "Unknown Artist"
            album_obj = detail.get("al") if isinstance(detail.get("al"), dict) else {}
            album_name = album_obj.get("name") or "Unknown Album"
            cover_url = album_obj.get("picUrl")
            track_no = int(detail.get("no") or 1)
            disc_raw = detail.get("cd")
            disc_no = 1
            if isinstance(disc_raw, str):
                if "/" in disc_raw:
                    disc_raw = disc_raw.split("/", 1)[0]
                if disc_raw.isdigit():
                    disc_no = int(disc_raw)
            publish_time = detail.get("publishTime")
            year = ""
            if isinstance(publish_time, int) and publish_time > 0:
                year = datetime.fromtimestamp(publish_time / 1000, tz=timezone.utc).strftime("%Y")

            audio_info = self._pick_audio(song_id)
            audio_url = audio_info.get("url")
            if not audio_url:
                raise RuntimeError(f"音频 URL 为空: song_id={song_id}")
            audio_type = audio_info.get("type") or "unknown"

            temp_input = self.temp_dir / f"{song_id}_raw.{audio_type if audio_type != 'unknown' else 'bin'}"
            raw_size = self._download_file(audio_url, temp_input)

            safe_artist = sanitize_filename(artists, 80)
            safe_title = sanitize_filename(name, 120)
            final_name = f"{safe_artist} - {safe_title}.m4a"
            final_path = self.audio_dir / final_name
            self._ffmpeg_convert_to_m4a(temp_input, final_path)

            lyric_text = self._fetch_lyric(song_id)
            cover_data: Optional[bytes] = None
            cover_content_type: Optional[str] = None
            if isinstance(cover_url, str) and cover_url.startswith("http"):
                cover_bytes = self._download_bytes(cover_url + "?param=1000y1000")
                cover_data = cover_bytes
                try:
                    # requests 会自动处理 content-type
                    head = self.session.head(cover_url, timeout=self.cfg.timeout)
                    if head.ok:
                        cover_content_type = head.headers.get("Content-Type")
                except Exception:  # noqa: BLE001
                    cover_content_type = None

            self._write_m4a_tags(
                final_path,
                title=name,
                artist=artists,
                album=album_name,
                album_artist=artists,
                track_no=track_no,
                disc_no=disc_no,
                year=year,
                lyrics=lyric_text,
                cover_data=cover_data,
                cover_content_type=cover_content_type,
            )

            if self.cfg.music_import:
                self._music_import(final_path)

            elapsed_ms = int((time.monotonic() - start_ts) * 1000)
            self._upsert_track(
                song_id,
                name=name,
                artist=artists,
                album=album_name,
                status="success",
                attempts=1,
                duration_ms=elapsed_ms,
                output_path=str(final_path),
                audio_url=audio_url,
                audio_type=audio_type,
                file_size=final_path.stat().st_size,
            )
            self.log_event(
                "INFO",
                "song_done",
                "单曲处理完成",
                song_id=song_id,
                name=name,
                artist=artists,
                audio_type=audio_type,
                raw_bytes=raw_size,
                output_path=str(final_path),
                elapsed_ms=elapsed_ms,
            )
        except Exception as exc:  # noqa: BLE001
            elapsed_ms = int((time.monotonic() - start_ts) * 1000)
            self._upsert_track(
                song_id,
                status="failed",
                attempts=1,
                duration_ms=elapsed_ms,
                last_error=str(exc),
            )
            self.log_event(
                "ERROR",
                "song_failed",
                "单曲处理失败",
                song_id=song_id,
                error=str(exc),
                elapsed_ms=elapsed_ms,
            )
        finally:
            for p in self.temp_dir.glob(f"{song_id}_raw.*"):
                try:
                    p.unlink()
                except OSError:
                    pass

    def summarize(self) -> dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS success_count,
              SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_count,
              AVG(CASE WHEN duration_ms IS NOT NULL THEN duration_ms END) AS avg_ms
            FROM tracks
            WHERE run_id = ?
            """,
            (self.run_id,),
        ).fetchone()

        failures = self.conn.execute(
            """
            SELECT song_id, name, last_error
            FROM tracks
            WHERE run_id = ? AND status = 'failed'
            ORDER BY updated_at DESC
            LIMIT 20
            """,
            (self.run_id,),
        ).fetchall()

        summary = {
            "run_id": self.run_id,
            "generated_at": utc_now(),
            "playlist": str(self.cfg.playlist_json),
            "output_dir": str(self.cfg.output_dir),
            "audio_dir": str(self.audio_dir),
            "metrics": {
                "total": int(row["total"] or 0),
                "success": int(row["success_count"] or 0),
                "failed": int(row["failed_count"] or 0),
                "avg_ms": int(row["avg_ms"] or 0),
            },
            "failures": [
                {
                    "song_id": int(item["song_id"]),
                    "name": item["name"],
                    "error": item["last_error"],
                }
                for item in failures
            ],
            "artifacts": {
                "events_log": str(self.log_file),
                "sqlite": str(self.db_path),
                "report": str(self.report_file),
            },
        }
        self.report_file.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return summary

    def finish_run(self, total: int, success: int, failed: int) -> None:
        self.conn.execute(
            """
            UPDATE runs
            SET finished_at = ?, total_tracks = ?, success_tracks = ?, failed_tracks = ?
            WHERE run_id = ?
            """,
            (utc_now(), total, success, failed, self.run_id),
        )
        self.conn.commit()

    def run(self) -> int:
        tracks = self._parse_playlist_tracks()
        if self.cfg.limit > 0:
            tracks = tracks[: self.cfg.limit]
        total = len(tracks)
        self.log_event(
            "INFO",
            "run_start",
            "开始执行 iPod touch 音频管线",
            playlist=str(self.cfg.playlist_json),
            total_tracks=total,
            output_dir=str(self.cfg.output_dir),
            level=self.cfg.level,
            bitrate=self.cfg.bitrate,
        )
        if self.cfg.report_only:
            summary = self.summarize()
            self.log_event("INFO", "report_only", "仅输出报告", summary=summary)
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return 0

        for index, track in enumerate(tracks, start=1):
            song_id = int(track.get("id"))
            name = str(track.get("name") or "")
            self.log_event(
                "INFO",
                "song_start",
                "开始处理单曲",
                index=index,
                total=total,
                song_id=song_id,
                name=name,
            )
            self.process_song(song_id, basic_name=name)

        summary = self.summarize()
        metrics = summary["metrics"]
        self.finish_run(
            total=metrics["total"],
            success=metrics["success"],
            failed=metrics["failed"],
        )
        self.log_event("INFO", "run_done", "管线执行完成", summary=summary)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if metrics["failed"] == 0 else 2


def parse_args(argv: list[str]) -> PipelineConfig:
    parser = argparse.ArgumentParser(
        description="将网易云歌单批量下载并转为 iPod touch 兼容 m4a 文件"
    )
    parser.add_argument(
        "--playlist-json",
        required=True,
        help="歌单 JSON 文件路径，需包含 tracks[].id",
    )
    parser.add_argument(
        "--output-dir",
        default="exports/ipod_touch",
        help="输出目录（音频、日志、数据库）",
    )
    parser.add_argument(
        "--api-base",
        default="http://127.0.0.1:3000",
        help="本地 API 服务地址",
    )
    parser.add_argument(
        "--cookie",
        default=os.getenv("NCM_COOKIE"),
        help="网易云 Cookie，优先使用参数，其次读取 NCM_COOKIE 环境变量",
    )
    parser.add_argument(
        "--level",
        default="standard",
        help="song/url/v1 的音质级别，例如 standard/exhigh/lossless",
    )
    parser.add_argument(
        "--bitrate",
        default="256k",
        help="转码 AAC 码率，例如 192k/256k",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="HTTP 请求失败重试次数",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="单次 HTTP 超时时间（秒）",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="仅输出最近运行的汇总报告，不执行下载和转码",
    )
    parser.add_argument(
        "--music-import",
        action="store_true",
        help="处理完每首后自动导入 Music.app",
    )
    parser.add_argument(
        "--music-playlist",
        default="NCM iPod Sync",
        help="导入 Music.app 时的播放列表名称",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="仅处理前 N 首，0 表示处理全部",
    )
    args = parser.parse_args(argv)

    return PipelineConfig(
        playlist_json=Path(args.playlist_json).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        api_base=args.api_base,
        cookie=args.cookie,
        level=args.level,
        bitrate=args.bitrate,
        retries=max(1, args.retries),
        timeout=max(5, args.timeout),
        report_only=args.report_only,
        music_import=bool(args.music_import),
        music_playlist=args.music_playlist,
        limit=max(0, args.limit),
    )


def check_dependencies() -> None:
    ffmpeg = subprocess.run(
        ["ffmpeg", "-version"],
        capture_output=True,
        text=True,
        check=False,
    )
    if ffmpeg.returncode != 0:
        raise SystemExit("未检测到 ffmpeg，请先安装后再运行本脚本")


def main(argv: list[str]) -> int:
    cfg = parse_args(argv)
    if not cfg.playlist_json.exists():
        raise SystemExit(f"playlist 文件不存在: {cfg.playlist_json}")
    check_dependencies()
    pipeline = ObservablePipeline(cfg)
    return pipeline.run()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
