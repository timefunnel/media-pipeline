import test from 'node:test';
import assert from 'node:assert/strict';
import { Globals } from 'danmu-api-server/danmu_api/configs/globals.js';
import { exactTitle, stableEpisodeId, search, comments, assertSourceSuccess } from './bridge.mjs';

test('exact title and season: intrinsic numbers, explicit seasons, no fuzzy match', () => {
  for (const [text, title, season, expected] of [
    ['怪兽8号', '怪兽8号', 1, true], ['86', '86', 1, true],
    ['爱情公寓4', '爱情公寓', 4, true], ['爱情公寓4', '爱情公寓', 1, false],
    ['爱情公寓', '爱情公寓', 4, false], ['爱情公寓 第四季', '爱情公寓', 4, true],
    ['遮天剧场版', '遮天', 1, false], ['爱情公寓 第三季', '爱情公寓', 4, false],
  ]) assert.equal(exactTitle({ title: text }, { title, season }), expected, text);
});

test('stable references survive restarts and distinguish providers', () => {
  const url = 'https://www.iqiyi.com/v_19rrgzfn2k.html';
  assert.equal(stableEpisodeId('iqiyi', url), stableEpisodeId('iqiyi', url));
  assert.match(stableEpisodeId('iqiyi', url), /^\d+$/);
  assert.notEqual(stableEpisodeId('iqiyi', url), stableEpisodeId('tencent', url));
});

test('iqiyi receives target season, rejects another season from combined album', async () => {
  let requested;
  const fake = { search: async () => [{ title: '爱情公寓4', mediaId: 'album', type: '电视剧' }],
    getEpisodes: async (id, season) => {
      requested = [id, season];
      return [{ order: 16, title: '爱情公寓3第16集', link: 'wrong' },
        { order: 16, title: '爱情公寓4第16集 超级英雄', link: 'https://www.iqiyi.com/v_19rrgzfn2k.html' }];
    } };
  const result = await search(fake, 'iqiyi', { title: '爱情公寓', season: 4, episode: 16 });
  assert.deepEqual(requested, ['album', 4]);
  assert.equal(result[0].episodes.length, 1);
  assert.equal(result[0].episodes[0].url, 'https://www.iqiyi.com/v_19rrgzfn2k.html');
});

test('animation supported, title prefix cannot accept another numbered season', async () => {
  const fake = { search: async () => [{ title: '遮天', mediaId: 'album', type: '动漫' }],
    getEpisodes: async () => [{ title: '148', unionTitle: '遮天 第148集', vid: 'correct' },
      { title: '148', unionTitle: '遮天2 第148集', vid: 'wrong' }] };
  const result = await search(fake, 'tencent', { title: '遮天', season: 1, episode: 148 });
  assert.equal(result[0].episodes.length, 1);
  assert.equal(result[0].episodes[0].url, 'https://v.qq.com/x/cover/album/correct.html');
});

test('movies require matching release year, no movie-series crossover', async () => {
  let calls = 0;
  const fake = { search: async () => [{ title: '重映', mediaId: 'old', type: '电影', year: 1990 },
    { title: '重映', mediaId: 'new', type: '电影', year: 2024 }, { title: '重映', mediaId: 'tv', type: '电视剧', year: 2024 }],
    getEpisodes: async () => { calls++; return [{ order: 1, title: '正片', link: 'https://www.iqiyi.com/v_movie.html' }]; } };
  const result = await search(fake, 'iqiyi', { title: '重映', season: 0, episode: 0, year: 2024 });
  assert.equal(calls, 1);
  assert.equal(result[0].animeId, 'new');
});

test('segment downloads stay at concurrency two', async () => {
  Globals.init({ LOG_LEVEL: 'error', LIKE_SWITCH: 'false', DANMU_LIMIT: '0', GROUP_MINUTE: '0' });
  let active = 0, maximum = 0;
  const fake = { getEpisodeDanmuSegments: async () => ({ segmentList: [1, 2, 3, 4, 5] }),
    getEpisodeSegmentDanmu: async () => {
      maximum = Math.max(maximum, ++active);
      await new Promise(resolve => setTimeout(resolve, 5));
      active--; return [];
    }, formatComments: items => items };
  await comments(fake, 'iqiyi', 'unused');
  assert.equal(maximum, 2);
});

test('upstream caught error and risk-control failure cannot turn into a successful empty cache', () => {
  Globals.logBuffer = [{ level: 'error', message: 'network failed' }];
  assert.throws(() => assertSourceSuccess('tencent'), /network failed/);
  Globals.logBuffer = [{ level: 'info', message: '搜索接口风控，重试后仍失败' }];
  assert.throws(() => assertSourceSuccess('iqiyi'), /仍失败/);
  Globals.logBuffer = [];
});
