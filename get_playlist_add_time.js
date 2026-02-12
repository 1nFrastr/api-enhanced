/**
 * 获取歌单中每首歌的添加时间
 * 用法: node get_playlist_add_time.js <歌单ID>
 * 示例: node get_playlist_add_time.js 3778678
 */

const axios = require('axios');

const API_BASE = 'http://localhost:3000';

/**
 * 将Unix时间戳转换为可读日期
 * @param {number} timestamp - 毫秒级时间戳
 * @returns {string} 格式化后的日期时间
 */
function formatTime(timestamp) {
  const date = new Date(timestamp);
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
}

/**
 * 获取歌单详情（包含trackIds和添加时间）
 * @param {string|number} playlistId - 歌单ID
 * @returns {Promise<Object>}
 */
async function getPlaylistDetail(playlistId) {
  try {
    const response = await axios.get(`${API_BASE}/playlist/detail`, {
      params: { id: playlistId }
    });

    if (response.data.code !== 200) {
      throw new Error(`API返回错误: ${response.data.message || '未知错误'}`);
    }

    return response.data.playlist;
  } catch (error) {
    throw new Error(`获取歌单详情失败: ${error.message}`);
  }
}

/**
 * 获取歌曲详情
 * @param {number[]} ids - 歌曲ID数组
 * @returns {Promise<Object[]>}
 */
async function getSongDetails(ids) {
  try {
    // 将ID数组格式化为API需要的格式
    const c = JSON.stringify(ids.map(id => ({ id })));
    const response = await axios.get(`${API_BASE}/song/detail`, {
      params: { ids: ids.join(','), c: c }
    });

    if (response.data.code !== 200) {
      throw new Error(`API返回错误: ${response.data.message || '未知错误'}`);
    }

    return response.data.songs || [];
  } catch (error) {
    throw new Error(`获取歌曲详情失败: ${error.message}`);
  }
}

/**
 * 获取歌单中所有歌曲及其添加时间
 * @param {string|number} playlistId - 歌单ID
 */
async function getPlaylistTracksWithAddTime(playlistId) {
  try {
    console.log(`正在获取歌单 ${playlistId} 的信息...\n`);

    // 1. 获取歌单详情（包含trackIds和添加时间at）
    const playlist = await getPlaylistDetail(playlistId);

    console.log(`歌单名称: ${playlist.name}`);
    console.log(`歌单作者: ${playlist.creator?.nickname || '未知'}`);
    console.log(`歌曲数量: ${playlist.trackCount}`);
    console.log(`创建时间: ${formatTime(playlist.createTime)}`);
    console.log(`更新时间: ${formatTime(playlist.updateTime)}`);
    console.log('\n' + '='.repeat(80) + '\n');

    const trackIds = playlist.trackIds;

    if (!trackIds || trackIds.length === 0) {
      console.log('歌单中没有歌曲');
      return;
    }

    // 2. 获取歌曲详情（分批获取，每次最多100首）
    const batchSize = 100;
    const allTracks = [];

    for (let i = 0; i < trackIds.length; i += batchSize) {
      const batch = trackIds.slice(i, i + batchSize);
      const ids = batch.map(item => item.id);
      const songs = await getSongDetails(ids);

      // 将歌曲详情与添加时间关联
      for (const song of songs) {
        const trackInfo = trackIds.find(t => t.id === song.id);
        allTracks.push({
          id: song.id,
          name: song.name,
          artists: song.ar?.map(a => a.name).join(', ') || '未知',
          album: song.al?.name || '未知',
          addTime: trackInfo?.at || 0,
          addTimeFormatted: trackInfo?.at ? formatTime(trackInfo.at) : '未知'
        });
      }

      console.log(`已获取 ${Math.min(i + batchSize, trackIds.length)}/${trackIds.length} 首歌曲...`);
    }

    // 3. 按添加时间排序（最新的在前面）
    allTracks.sort((a, b) => b.addTime - a.addTime);

    // 4. 输出结果
    console.log('\n' + '='.repeat(80));
    console.log('歌曲列表（按添加时间从新到旧排序）：');
    console.log('='.repeat(80) + '\n');

    allTracks.forEach((track, index) => {
      console.log(`${index + 1}. ${track.name}`);
      console.log(`   歌手: ${track.artists}`);
      console.log(`   专辑: ${track.album}`);
      console.log(`   添加时间: ${track.addTimeFormatted}`);
      console.log('');
    });

    // 5. 保存到文件
    const fs = require('fs');
    const outputFile = `playlist_${playlistId}_tracks.json`;
    fs.writeFileSync(outputFile, JSON.stringify({
      playlistInfo: {
        id: playlist.id,
        name: playlist.name,
        author: playlist.creator?.nickname,
        trackCount: playlist.trackCount,
        createTime: formatTime(playlist.createTime),
        updateTime: formatTime(playlist.updateTime)
      },
      tracks: allTracks
    }, null, 2));

    console.log(`\n结果已保存到文件: ${outputFile}`);

  } catch (error) {
    console.error('错误:', error.message);
    process.exit(1);
  }
}

// 主程序
const playlistId = process.argv[2];

if (!playlistId) {
  console.log('用法: node get_playlist_add_time.js <歌单ID>');
  console.log('示例: node get_playlist_add_time.js 3778678');
  console.log('\n提示: 可以从网易云音乐网页版URL中获取歌单ID');
  console.log('      例如: https://music.163.com/#/playlist?id=3778678');
  process.exit(1);
}

getPlaylistTracksWithAddTime(playlistId);
