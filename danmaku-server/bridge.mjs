// 复用固定提交的 danmu_api 源适配器；此入口从不同时搜索不同源。
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { Globals } from 'danmu-api-server/danmu_api/configs/globals.js';
import TencentSource from 'danmu-api-server/danmu_api/sources/tencent.js';
import IqiyiSource from 'danmu-api-server/danmu_api/sources/iqiyi.js';
import { extractSeasonNumberFromAnimeTitle } from 'danmu-api-server/danmu_api/utils/common-util.js';
import { convertToDanmakuJson } from 'danmu-api-server/danmu_api/utils/danmu-util.js';

const normalize = value => String(value || '').normalize('NFKC').replace(/\s+/g, ' ').trim().toLowerCase();

export function exactTitle(candidate, target) {
  const wanted = [target.title, target.original_title].filter(Boolean);
  const texts = [candidate.title, ...(candidate.aliases || [])].filter(Boolean);
  return texts.some(text => wanted.some(title => {
    // 作品名本身含数字时必须先比较完整名称，不能把“怪兽8号”等数字当季号。
    if (normalize(text) === normalize(title)) {
      const explicit = /第\s*([0-9一二三四五六七八九十]+)\s*季|(?:Season|\bS)\s*\d+/i.test(text);
      return explicit ? extractSeasonNumberFromAnimeTitle(text).season === target.season : target.season <= 1;
    }
    const parsed = extractSeasonNumberFromAnimeTitle(text);
    return parsed.season === target.season && normalize(parsed.baseTitle) === normalize(title);
  }));
}

export function stableEpisodeId(source, url) {
  // 正整数兼容现有客户端/数据库；关联的是稳定源站 URL，不是上游临时计数器。
  return (BigInt('0x' + createHash('sha256').update(source + '\n' + url).digest('hex').slice(0, 15)) + 1n).toString();
}

export async function search(source, provider, target) {
  const title = target.title;
  const query = target.season > 1 ? `${title} 第${target.season}季` : title;
  const programs = await source.search(query);
  const movie = !target.season && !target.episode;
  const candidates = programs.filter(program => exactTitle(program, target) &&
    (movie ? program.type === '电影' && target.year > 0 && Number(program.year) === target.year
      : ['电视剧', '动漫', '纪录片'].includes(program.type)));
  const animes = [];
  for (const program of candidates) {
    const episodes = await source.getEpisodes(program.mediaId, provider === 'tencent' ? (program.chapterContexts || []) : target.season || null);
    const selected = episodes.filter(ep => movie ? episodes.length === 1 :
      Number.isInteger(Number(provider === 'tencent' ? ep.title : ep.order)) && Number(provider === 'tencent' ? ep.title : ep.order) > 0);
    const entries = [];
    for (const ep of selected) {
      const url = provider === 'tencent' ? `https://v.qq.com/x/cover/${program.mediaId}/${ep.vid}.html` : ep.link;
      const episodeTitle = provider === 'tencent' ? ep.unionTitle : ep.title;
      // 多季合辑中不能只按数组下标选集；明确带作品名的分集必须再次核对作品与季。
      if (!movie) {
        const remainder = normalize(episodeTitle).slice(normalize(program.title).length);
        if (!normalize(episodeTitle).startsWith(normalize(program.title)) || /^\d/.test(remainder)) continue;
      }
      if (!url) throw new Error(`${provider}: selected episode has no URL`);
      entries.push({ episodeId: stableEpisodeId(provider, url), episodeTitle,
        episodeNumber: movie ? '' : String(Number(provider === 'tencent' ? ep.title : ep.order)), url });
    }
    if (entries.length) animes.push({ animeId: program.mediaId, animeTitle: program.title, type: movie ? 'movie' : 'tv', typeDescription: program.type, episodes: entries });
  }
  return animes;
}

export function assertSourceSuccess(provider) {
  const errors = Globals.logBuffer.filter(entry => entry.level === 'error' ||
    /响应为空|重试后仍失败/.test(entry.message));
  if (errors.length) throw new Error(`${provider}: ${errors.map(entry => entry.message).join('; ').slice(0, 1500)}`);
}

export async function comments(source, provider, url) {
  const segments = await source.getEpisodeDanmuSegments(url);
  assertSourceSuccess(provider);
  if (!segments || !Array.isArray(segments.segmentList)) throw new Error(`${provider}: invalid segment response`);
  const raw = [];
  let next = 0;
  // 只在当前已命中的源内抓取分片，最多 2 个并发；任何分片错误都不保存部分成功结果。
  await Promise.all([0, 1].map(async () => {
    while (next < segments.segmentList.length) {
      const batch = await source.getEpisodeSegmentDanmu(segments.segmentList[next++]);
      assertSourceSuccess(provider);
      if (!Array.isArray(batch)) throw new Error(`${provider}: invalid segment comments`);
      raw.push(...batch);
    }
  }));
  return convertToDanmakuJson(source.formatComments(raw), provider === 'tencent' ? 'qq' : 'qiyi');
}

export async function run(action, provider, data) {
  if (!['tencent', 'iqiyi'].includes(provider)) throw new Error('unsupported native source');
  Globals.init({ SOURCE_ORDER: provider, LOG_LEVEL: 'info', VOD_REQUEST_TIMEOUT: '15000', STRICT_TITLE_MATCH: 'true',
    LOCAL_CACHE_ENABLED: 'false', DANMU_LIMIT: '0', GROUP_MINUTE: '0', LIKE_SWITCH: 'false' });
  const source = provider === 'tencent' ? new TencentSource() : new IqiyiSource();
  let result;
  if (action === 'search') result = await search(source, provider, data);
  else if (action === 'comment') {
    const parsed = new URL(data.url);
    const allowed = provider === 'tencent' ? parsed.hostname === 'v.qq.com' && /^\/x\/cover\/[a-z0-9]+\/[a-z0-9]+\.html$/.test(parsed.pathname)
      : parsed.hostname === 'www.iqiyi.com' && /^\/v_[a-z0-9]+\.html$/.test(parsed.pathname);
    if (parsed.protocol !== 'https:' || !allowed || parsed.username || parsed.password) throw new Error('invalid source episode URL');
    result = { comments: await comments(source, provider, data.url) };
  } else throw new Error('unknown bridge action');
  // 上游源库可能记录错误后返回 []，必须把这种失败显式带回主服务，不能缓存为空。
  assertSourceSuccess(provider);
  return result;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  // 源库日志走 stderr，stdout 只承载一个 JSON 对象。
  console.log = console.info = console.debug = console.warn = (...args) => console.error(...args);
  try {
    let input = '';
    for await (const chunk of process.stdin) input += chunk;
    const { action, source, data } = JSON.parse(input);
    const result = await run(action, source, data);
    process.stdout.write(JSON.stringify(result));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
