/**
 * 对比「我喜欢的音乐」历史快照与当前歌单，找出被取消喜欢的那一首
 * 用法: node find_removed_like.js [歌单ID]
 * 默认歌单ID: 874935036（与 playlist_874935036_tracks.json 对应）
 * 需要本地 API 运行在 localhost:3000；若为「我喜欢的音乐」请设置 COOKIE 环境变量
 */

const axios = require('axios');
const fs = require('fs');
const path = require('path');

const API_BASE = process.env.API_BASE || 'http://localhost:3000';
const DEFAULT_PLAYLIST_ID = 874935036;

function getCookie() {
  return process.env.COOKIE || process.env.NETEASE_COOKIE || '';
}

async function getPlaylistDetail(playlistId) {
  const opts = { params: { id: playlistId } };
  const cookie = getCookie();
  if (cookie) opts.headers = { Cookie: cookie };
  const response = await axios.get(`${API_BASE}/playlist/detail`, opts);
  if (response.data.code !== 200) {
    throw new Error(response.data.message || '获取歌单失败');
  }
  return response.data.playlist;
}

function loadLocalSnapshot(playlistId) {
  const file = path.join(__dirname, `playlist_${playlistId}_tracks.json`);
  if (!fs.existsSync(file)) {
    throw new Error(`本地快照不存在: ${file}`);
  }
  return JSON.parse(fs.readFileSync(file, 'utf-8'));
}

async function main() {
  const playlistId = process.argv[2] || DEFAULT_PLAYLIST_ID;

  console.log('加载本地快照 playlist_%s_tracks.json ...', playlistId);
  const snapshot = loadLocalSnapshot(playlistId);
  const oldTracks = snapshot.tracks || [];
  const oldIds = new Set(oldTracks.map((t) => String(t.id)));
  const oldCount = oldTracks.length;

  console.log('快照中歌曲数: %d', oldCount);
  console.log('正在获取当前歌单 %s ...', playlistId);

  const playlist = await getPlaylistDetail(playlistId);
  const trackIds = playlist.trackIds || [];
  const currentIds = new Set(trackIds.map((t) => String(t.id)));
  const currentCount = trackIds.length;

  console.log('当前歌单歌曲数: %d', currentCount);

  const missingIds = [...oldIds].filter((id) => !currentIds.has(id));
  if (missingIds.length === 0) {
    if (oldCount !== currentCount) {
      console.log('\n未找到「只在快照中、当前歌单没有」的歌曲（按 ID 对比）。');
      console.log('可能原因: 当前歌单比快照多了歌曲，或网易返回的 trackIds 与快照格式不一致。');
    } else {
      console.log('\n快照与当前歌单数量一致，没有发现被取消喜欢的歌曲。');
    }
    return;
  }

  const missing = oldTracks.filter((t) => missingIds.includes(String(t.id)));
  console.log('\n被取消喜欢的歌（共 %d 首）：\n', missing.length);
  missing.forEach((t, i) => {
    console.log('%d. %s', i + 1, t.name);
    console.log('   歌手: %s', t.artists || '-');
    console.log('   歌曲ID: %s', t.id);
    console.log('');
  });
}

main().catch((err) => {
  console.error('错误:', err.message);
  process.exit(1);
});
