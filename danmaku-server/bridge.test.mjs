import test from 'node:test';
import assert from 'node:assert/strict';
import { Globals } from 'danmu-api-server/danmu_api/configs/globals.js';
import TencentSource from 'danmu-api-server/danmu_api/sources/tencent.js';
import { exactTitle, stableEpisodeId, search, comments, assertSourceSuccess, youkuEpisodeNumber, safeError, run, sourceErrorEnvelope } from './bridge.mjs';
import StrictYoukuSource, { parseYoukuSegment, parseYoukuTitle } from './youku.mjs';
import { SourceRiskControlError, responseRiskReason, withSourceRiskGuard } from './risk.mjs';
import { httpGet } from 'danmu-api-server/danmu_api/utils/http-util.js';

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

test('Tencent 2D and 3D animation tags preserve exact title, season and episode checks', async () => {
  const adapter = new TencentSource();
  for (const dimension of ['', '2D', '3D']) {
    const program = adapter.filterTencentSearchItem({ doc: { id: 'album' }, videoInfo: {
      title: '遮天', year: 2023, typeName: '动漫', playSites: [{ enName: 'qq' }],
      coverDoc: { richTags: dimension ? [{ text: `${dimension}动画` }] : [] },
    } }, '遮天');
    assert.equal(program.type, `${dimension}动漫`);
    const calls = [];
    const fake = { search: async () => [program, { ...program, title: '遮天剧场版' },
      { ...program, title: '遮天 第二季' }],
      getEpisodes: async (id, chapters) => {
        calls.push([id, chapters]);
        return [{ title: '148', unionTitle: '遮天 第148集', vid: 'correct' },
          { title: '148', unionTitle: '遮天2 第148集', vid: 'wrong-season' },
          { title: '148', unionTitle: '另一个作品 第148集', vid: 'wrong-title' },
          { title: '预告', unionTitle: '遮天 第148集预告', vid: 'trailer' }];
      } };
    const result = await search(fake, 'tencent', { title: '遮天', season: 1, episode: 148, year: 2023 });
    assert.deepEqual(calls, [['album', []]], dimension);
    assert.equal(result.length, 1, dimension);
    assert.equal(result[0].typeDescription, `${dimension}动漫`);
    assert.equal(result[0].episodes.length, 1);
    assert.equal(result[0].episodes[0].episodeNumber, '148');
    assert.equal(result[0].episodes[0].url, 'https://v.qq.com/x/cover/album/correct.html');
  }
});

test('animation type additions do not admit unknown types or movie-series crossover', async () => {
  const fake = { search: async () => ['电影', '综艺', '3D综艺', '短剧', '3D动漫解说', '未知动漫'].map(type =>
    ({ title: '遮天', type, mediaId: 'rejected', year: 2023 })),
    getEpisodes: async () => { assert.fail('rejected program must not fetch episodes'); } };
  assert.deepEqual(await search(fake, 'tencent', { title: '遮天', season: 1, episode: 148, year: 2023 }), []);
  fake.search = async () => ['动漫', '2D动漫', '3D动漫'].map(type =>
    ({ title: '遮天', type, mediaId: 'rejected', year: 2023 }));
  assert.deepEqual(await search(fake, 'tencent', { title: '遮天', season: 0, episode: 0, year: 2023 }), []);
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

test('segment downloads obey default six and configurable bounded concurrency', async () => {
  Globals.init({ LOG_LEVEL: 'error', LIKE_SWITCH: 'false', DANMU_LIMIT: '0', GROUP_MINUTE: '0' });
  let active = 0, maximum = 0;
  const fake = { getEpisodeDanmuSegments: async () => ({ segmentList: Array.from({ length: 13 }, (_, index) => index) }),
    getEpisodeSegmentDanmu: async () => {
      maximum = Math.max(maximum, ++active);
      await new Promise(resolve => setTimeout(resolve, 5));
      active--; return [];
    }, formatComments: items => items };
  for (const limit of [2, 6, 8]) {
    maximum = 0;
    await comments(fake, 'iqiyi', 'unused', limit);
    assert.equal(maximum, limit);
  }
  await assert.rejects(comments(fake, 'iqiyi', 'unused', 9), /invalid segment concurrency/);
});

test('upstream caught error and risk-control failure cannot turn into a successful empty cache', () => {
  Globals.logBuffer = [{ level: 'error', message: 'network failed' }];
  assert.throws(() => assertSourceSuccess('tencent'), /network failed/);
  Globals.logBuffer = [{ level: 'info', message: '搜索接口风控，重试后仍失败' }];
  assert.throws(() => assertSourceSuccess('iqiyi'), /仍失败/);
  Globals.logBuffer = [];
});

test('Youku uses stage and original episode title, never seq or list position', async () => {
  const title = '爱情公寓 第一季';
  for (const [ep, expected] of [
    [{ stage: '1', seq: '99', title: `${title} 01` }, 1],
    [{ stage: '2', title: `${title} 第2集` }, 2],
    [{ stage: '1', title: '爱情公寓 第二季 01' }, null],
    [{ stage: '2', title: `${title} 01` }, null],
    [{ seq: '1', title: `${title} 01` }, null],
    [{ stage: '1.5', title: `${title} 01` }, null],
    [{ stage: '1', title: `${title} 01 预告` }, null],
    [{ stage: '1', title: `${title} 01花絮` }, null],
  ]) assert.equal(youkuEpisodeNumber(ep, title), expected);
  const fake = { search: async () => [{ title, type: '电视剧', mediaId: 'show' }],
    getEpisodes: async () => [
      { stage: '2', title: `${title} 02`, id: 'second' },
      { stage: '1', title: `${title} 01`, id: 'first', link: 'http://v.youku.com/v_show/id_first.html' },
      { stage: '3', title: '爱情公寓 第二季 03', id: 'wrong' },
    ] };
  const result = await search(fake, 'youku', { title: '爱情公寓', season: 1, episode: 1 });
  assert.deepEqual(result[0].episodes.map(ep => ep.episodeNumber), ['2', '1']);
  assert.equal(result[0].episodes[1].url, 'https://v.youku.com/v_show/id_first.html');
  fake.getEpisodes = async () => [{ stage: '1', title: `${title} 01`, id: 'first', link: 'https://v.youku.com/v_show/id_other.html' }];
  await assert.rejects(search(fake, 'youku', { title: '爱情公寓', season: 1, episode: 1 }), /disagree/);
});

const loveDeathTarget = { title: 'Love, Death & Robots', original_title: 'Love, Death & Robots',
  season: 1, episode: 1, year: 2019 };

function youkuProgram(source, title, id = 'show', year = 2019) {
  return source.filterYoukuSearchItem({ commonData: { isYouku: 1, showId: id,
    titleDTO: { displayName: title }, feature: `${year} 动漫`, cats: '动漫', episodeTotal: 18 } }, loveDeathTarget.title);
}

test('Youku parses only an explicit trailing alias annotation and preserves title punctuation', () => {
  for (const suffix of ['(别名:Love, Death & Robots)', '(别名：Love, Death & Robots)', '（别名：Love, Death & Robots）']) {
    const parsed = parseYoukuTitle(`爱，死亡和机器人 第一季${suffix}`);
    assert.equal(parsed.title, '爱，死亡和机器人 第一季');
    assert.equal(parsed.season, 1);
    assert.deepEqual(parsed.aliases, ['爱，死亡和机器人', 'Love, Death & Robots']);
  }
  for (const title of ['86', '怪兽8号', '作品(导演剪辑版)', '作品(别名：)', '作品(别名：Another）',
    '作品(别名：Another) 花絮']) {
    const parsed = parseYoukuTitle(title);
    assert.equal(parsed.title, title);
    assert.deepEqual(parsed.aliases, []);
    assert.equal(parsed.season, null);
  }
  const parsed = parseYoukuTitle('爱、死亡和机器人 第三季(别名：Love, Death & Robots Season 3)');
  assert.equal(parsed.season, 3);
  assert.deepEqual(parsed.aliases, ['爱、死亡和机器人', 'Love, Death & Robots Season 3', 'Love, Death & Robots']);
});

test('Youku explicit aliases share the canonical season and never bypass season conflicts', () => {
  const source = new StrictYoukuSource();
  const first = youkuProgram(source, '爱，死亡和机器人 第一季(别名:Love, Death & Robots)');
  assert.equal(first.title, '爱，死亡和机器人 第一季');
  assert.equal(first.type, '动漫');
  assert.equal(first.year, 2019);
  assert.equal(exactTitle(first, loveDeathTarget), true);
  assert.equal(exactTitle(first, { ...loveDeathTarget, title: '爱，死亡和机器人', original_title: '' }), true);
  assert.equal(exactTitle(first, { ...loveDeathTarget, season: 3 }), false);
  assert.equal(exactTitle(first, { ...loveDeathTarget, title: 'Love, Death & Robots Movie', original_title: '' }), false);
  assert.equal(exactTitle(first, { ...loveDeathTarget, title: 'Love Death Robots', original_title: '' }), false);
  for (const alias of ['Love, Death & Robots', 'Love, Death & Robots Season 3']) {
    const third = youkuProgram(source, `爱、死亡和机器人 第三季(别名：${alias})`, 'third', 2022);
    assert.equal(exactTitle(third, loveDeathTarget), false);
    assert.equal(exactTitle(third, { ...loveDeathTarget, season: 3 }), true);
  }
  assert.throws(() => youkuProgram(source, '爱，死亡和机器人 第一季(别名：Love, Death & Robots Season 3)'), /season.*conflict/);
  assert.throws(() => parseYoukuTitle('作品 第0季(别名：Another)'), /invalid explicit title season/);
});

test('Youku alias match selects canonical season and still checks episode stage, title and URL', async () => {
  const source = new StrictYoukuSource();
  const first = youkuProgram(source, '爱，死亡和机器人 第一季(别名：Love, Death & Robots)', 'first');
  const third = youkuProgram(source, '爱、死亡和机器人 第三季(别名：Love, Death & Robots Season 3)', 'third', 2022);
  source.search = async () => [third, first];
  const calls = [];
  source.getEpisodes = async (id, season) => {
    calls.push([id, season]);
    const title = id === 'first' ? first.title : third.title;
    return [{ id: 'one', stage: '1', seq: '99', title: `${title} 01` },
      { id: 'two', stage: '2', title: `${title} 02` },
      { id: 'wrong-season', stage: '1', title: '爱，死亡和机器人 第二季 01' },
      { id: 'wrong-stage', stage: '2', title: `${title} 01` },
      { id: 'missing-stage', seq: '1', title: `${title} 01` },
      { id: 'trailer', stage: '1', title: `${title} 01 预告` }];
  };
  for (const season of [1, 3]) {
    const result = await search(source, 'youku', { ...loveDeathTarget, season });
    assert.equal(result.length, 1);
    assert.equal(result[0].animeTitle, season === 1 ? first.title : third.title);
    assert.deepEqual(result[0].episodes.map(ep => ep.episodeNumber), ['1', '2']);
    assert.equal(result[0].episodes[0].url, 'https://v.youku.com/v_show/id_one.html');
  }
  assert.deepEqual(calls, [['first', 1], ['third', 3]]);
  // 真实接口可能只有作品卡而没有可用分集；别名命中不能凭空生成节目编号。
  source.getEpisodes = async () => [];
  assert.deepEqual(await search(source, 'youku', loveDeathTarget), []);
  source.getEpisodes = async () => [{ id: 'one', stage: '1', title: `${first.title} 01`,
    link: 'https://v.youku.com/v_show/id_different.html' }];
  await assert.rejects(search(source, 'youku', loveDeathTarget), /disagree/);
});

test('Youku accepts successful empty segments, rejects API errors and incomplete results', async () => {
  const payload = { ret: ['SUCCESS::调用成功'], data: { result: JSON.stringify({ code: 1, data: { result: [] } }) } };
  assert.deepEqual(parseYoukuSegment(payload), []);
  assert.throws(() => parseYoukuSegment({ ...payload, ret: ['FAIL_SYS_TOKEN_EXPIRED::secret detail'] }), /FAIL_SYS_TOKEN_EXPIRED/);
  for (const bad of [{}, { ...payload, ret: [] }, { ...payload, ret: ['FAIL_SYS_TOKEN_EXPIRED'] },
    { ...payload, data: { result: '{' } },
    { ...payload, data: { result: JSON.stringify({ code: -1, data: { result: [] } }) } },
    { ...payload, data: { result: JSON.stringify({ code: 0, data: { result: [] } }) } },
    { ...payload, data: { result: JSON.stringify({ code: 1, data: {} }) } },
  ]) assert.throws(() => parseYoukuSegment(bad));
  const source = new StrictYoukuSource(async () => ({ data: payload }));
  assert.deepEqual(await source.getEpisodeSegmentDanmu({ url: 'unused', data: '{}' }), []);
  const converted = await comments({ getEpisodeDanmuSegments: async () => ({ segmentList: [1] }),
    getEpisodeSegmentDanmu: async () => [{ playat: 1500, content: '优酷弹幕', propertis: '{"pos":1,"color":65280}' }],
    formatComments: entries => source.formatComments(entries) }, 'youku', 'unused');
  assert.equal(converted[0].m, '优酷弹幕');
  assert.match(converted[0].p, /^1\.50,5,65280,/);
});

test('Youku episode pages are complete and bounded; local anonymous identity fallback is disabled', async () => {
  const source = new StrictYoukuSource();
  const calls = [];
  source._getEpisodesPage = async (id, page, size) => {
    calls.push(page);
    return { total: 101, videos: Array.from({ length: page === 1 ? size : 1 }, (_, i) => ({ id: `${page}-${i}` })) };
  };
  assert.equal((await source.getEpisodes('show')).length, 101);
  assert.deepEqual(calls, [1, 2]);
  for (const bad of [null, {}, { total: 1, videos: [] }, { total: 10001, videos: [] },
    { total: 2, videos: [{ id: 'same' }, { id: 'same' }] }]) {
    source._getEpisodesPage = async () => bad;
    await assert.rejects(source.getEpisodes('show'));
  }
  assert.throws(() => source._getFallbackCna(), /disabled/);
});

test('one failed segment stops scheduling, waits for active segments, and returns no partial result', async () => {
  Globals.logBuffer = [];
  let calls = 0, active = 0;
  const fake = { getEpisodeDanmuSegments: async () => ({ segmentList: [0, 1, 2, 3, 4] }),
    getEpisodeSegmentDanmu: async index => {
      calls++; active++;
      try {
        if (index === 0) throw new Error('segment failed');
        await new Promise(resolve => setTimeout(resolve, 10));
        return [];
      } finally { active--; }
    }, formatComments: entries => entries };
  await assert.rejects(comments(fake, 'youku', 'unused', 2), /segment failed/);
  assert.equal(calls, 2);
  assert.equal(active, 0);
});

test('worker resets request logs and rejects noncanonical Youku episode URLs without fetching', async () => {
  Globals.logBuffer = [{ level: 'error', message: 'previous request error' }];
  await assert.rejects(run('invalid', 'youku', {}), /unknown bridge action/);
  assert.deepEqual(Globals.logBuffer, []);
  for (const url of ['https://example.com/v_show/id_first.html', 'http://v.youku.com/v_show/id_first.html',
    'https://v.youku.com/v_show/id_first.html?token=secret', 'https://user@v.youku.com/v_show/id_first.html']) {
    await assert.rejects(run('comment', 'youku', { url }), /invalid source episode URL/);
  }
  const error = safeError('_m_h5_tk: secret; https://acs.youku.com/path/?sign=anothersecret');
  assert.ok(!error.includes('secret'));
  assert.ok(!error.includes('?'));
});

test('risk detection only uses failed status and error fields, never titles or comment content', () => {
  const url = 'https://mesh.if.iqiyi.com/portal/lw/search/homePageV3';
  for (const code of ['-1', -1]) assert.equal(responseRiskReason('iqiyi', url, { code }), 'iqiyi_search_risk_control');
  assert.equal(responseRiskReason('iqiyi', 'https://www.iqiyi.com/other', { code: -1 }), null);
  for (const payload of [{ ret: -1, msg: '请求过于频繁' }, { code: -1, message: '验证码' },
    { ret: ['FAIL_SYS_USER_VALIDATE::验证请求'] }, { ret: ['FAIL_SYS_RGV587_ERROR::受限'] }]) {
    assert.equal(responseRiskReason('youku', 'https://acs.youku.com/api', payload), 'explicit_risk_control');
  }
  for (const payload of [{ code: 0, message: '验证码' }, { ret: 0, msg: '验证码', data: [] },
    { ret: ['SUCCESS::调用成功'], data: { comments: [{ content: '风控、验证码、请求过于频繁' }] } },
    { ret: ['FAIL_SYS_TOKEN_EXPIRED::令牌过期'] }, { ret: -1, msg: '节目不存在' },
    { code: 0, data: { templates: [] } }]) {
    assert.equal(responseRiskReason('youku', 'https://acs.youku.com/api', payload), null);
  }
});

test('iqiyi actual adapter risk response stops at one HTTP call without its three-second retries', async t => {
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async url => {
    calls++;
    assert.equal(new URL(url).pathname, '/portal/lw/search/homePageV3');
    return new Response(JSON.stringify({ code: '-1', data: null }));
  });
  const original = globalThis.fetch;
  await assert.rejects(run('search', 'iqiyi', { title: '测试作品', season: 1, episode: 1 }), error =>
    error instanceof SourceRiskControlError && error.riskReason === 'iqiyi_search_risk_control');
  assert.equal(calls, 1);
  assert.equal(globalThis.fetch, original);
});

test('HTTP 403 and 429 survive caught errors and block attempted retries for all native providers', async t => {
  let calls = 0;
  let status;
  t.mock.method(globalThis, 'fetch', async () => { calls++; return new Response('', { status }); });
  for (const provider of ['tencent', 'iqiyi', 'youku']) for (status of [403, 429]) {
    calls = 0;
    await assert.rejects(withSourceRiskGuard(provider, async () => {
      for (let attempt = 0; attempt < 3; attempt++) {
        try { await fetch('https://example.test/segment'); } catch { /* 模拟第三方内部 catch + retry。 */ }
      }
      return [];
    }), error => error instanceof SourceRiskControlError && error.riskReason === `http_${status}`);
    assert.equal(calls, 1, `${provider}: ${status}`);
  }
});

test('pinned HTTP helper retry does not send another upstream request after risk control', async t => {
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async () => { calls++; return new Response('', { status: 429 }); });
  // 适配器的物理重试可能仍有一次本地退避，但第二次 fetch 被拦截，不回源。
  t.mock.method(console, 'log', () => {});
  await assert.rejects(withSourceRiskGuard('tencent', () =>
    httpGet('https://example.test/segment', { retries: 1 })),
  error => error instanceof SourceRiskControlError && error.riskReason === 'http_429');
  assert.equal(calls, 1);
});

test('risk aborts active segments, stops scheduling, and never returns partial comments', async t => {
  Globals.logBuffer = [];
  let calls = 0, aborted = 0;
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    calls++;
    if (url.endsWith('/0')) return new Response('', { status: 429 });
    return new Promise((resolve, reject) => {
      if (options.signal.aborted) { aborted++; reject(options.signal.reason); return; }
      options.signal.addEventListener('abort', () => { aborted++; reject(options.signal.reason); }, { once: true });
    });
  });
  const source = { getEpisodeDanmuSegments: async () => ({ segmentList: [0, 1, 2, 3, 4] }),
    getEpisodeSegmentDanmu: async index => {
      await fetch(`https://example.test/${index}`);
      return [];
    }, formatComments: entries => entries };
  await assert.rejects(withSourceRiskGuard('youku', () => comments(source, 'youku', 'unused', 2)),
    error => error instanceof SourceRiskControlError && error.riskReason === 'http_429');
  assert.equal(calls, 2);
  assert.equal(aborted, 1);
});

test('ordinary HTTP errors do not latch the risk guard and another provider request is unaffected', async t => {
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async () => { calls++; return new Response('', { status: 500 }); });
  const original = globalThis.fetch;
  await assert.rejects(withSourceRiskGuard('tencent', async () => {
    const response = await fetch('https://example.test/api');
    throw new Error(`HTTP ${response.status}`);
  }), /HTTP 500/);
  assert.equal(globalThis.fetch, original);
  assert.equal(await withSourceRiskGuard('iqiyi', async () => (await fetch('https://example.test/api')).status), 500);
  assert.equal(calls, 2);
});

test('successful empty responses, binary segments and normal Youku token bootstrap are unchanged', async t => {
  let payload;
  t.mock.method(globalThis, 'fetch', async () => new Response(payload));
  for (payload of [JSON.stringify({ code: 0, data: [] }), JSON.stringify({ ret: ['FAIL_SYS_TOKEN_EXPIRED'] }), 'not-json']) {
    const result = await withSourceRiskGuard('youku', async () => (await fetch('https://acs.youku.com/api')).text());
    assert.equal(result, payload);
  }
  payload = 'binary segment';
  assert.equal(new TextDecoder().decode(await withSourceRiskGuard('iqiyi', async () =>
    (await fetch('https://example.test/segment')).arrayBuffer())), payload);
});

test('structured worker risk error contains only a stable code and safe reason', () => {
  assert.deepEqual(sourceErrorEnvelope(new SourceRiskControlError('http_403')), {
    ok: false, error: 'danmaku_source_risk_control: http_403', code: 'danmaku_source_risk_control', risk_reason: 'http_403',
  });
  assert.deepEqual(sourceErrorEnvelope(new Error('https://example.test/api?token=secret failed')), {
    ok: false, error: 'https://example.test/api failed',
  });
});
