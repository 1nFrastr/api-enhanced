# iPod touch 自动化管线

本工具用于将歌单 JSON 批量转为 iPod touch 兼容的 `m4a` 文件，并内嵌元数据、封面、歌词。

## 1) 依赖安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r tools/requirements-ipod-touch.txt
```

系统依赖:

- `ffmpeg` (用于转码为 AAC)
- 本地 API 服务已启动（默认 `http://127.0.0.1:3000`）

## 2) 基础运行

```bash
python3 tools/ipod_touch_pipeline.py \
  --playlist-json "./playlist_874935036_tracks.json" \
  --output-dir "./exports/ipod_touch" \
  --api-base "http://127.0.0.1:3000" \
  --level "standard" \
  --bitrate "256k"
```

需要登录态时可传 Cookie：

```bash
python3 tools/ipod_touch_pipeline.py \
  --playlist-json "./playlist_874935036_tracks.json" \
  --cookie "MUSIC_U=xxx; __csrf=xxx"
```

或使用环境变量：

```bash
export NCM_COOKIE='MUSIC_U=xxx; __csrf=xxx'
python3 tools/ipod_touch_pipeline.py --playlist-json "./playlist_874935036_tracks.json"
```

## 3) 可观测性

每次运行会生成一个 `run_id`，目录如下：

- `exports/ipod_touch/runs/<run_id>/events.jsonl`：结构化事件日志
- `exports/ipod_touch/runs/<run_id>/report.json`：本次汇总报告
- `exports/ipod_touch/pipeline_state.sqlite3`：全量状态库
- `exports/ipod_touch/audio/*.m4a`：最终产出音频

状态库 `tracks.status`:

- `processing`
- `success`
- `failed`

失败会记录 `last_error`，可据此二次筛查。

## 4) 可选导入 Music.app

```bash
python3 tools/ipod_touch_pipeline.py \
  --playlist-json "./playlist_874935036_tracks.json" \
  --music-import \
  --music-playlist "NCM iPod Sync"
```

说明:

- 该选项会在每首处理完成后调用 `osascript` 导入 `Music.app`
- 是否能自动同步到设备取决于系统权限与设备连接状态

## 5) 常见问题

1. **音频 URL 为空**
   - 通常是版权/会员/地区限制
   - 传入登录 Cookie 后重试
   - 已内置 `song/url/v1 -> song/url` 降级逻辑

2. **ffmpeg 转码失败**
   - 检查 `ffmpeg -version`
   - 查看 `events.jsonl` 中 `song_failed` 的 `error`

3. **封面或歌词缺失**
   - 部分曲目本身无对应数据
   - 不影响音频文件产出
