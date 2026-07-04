import math
import re
import urllib.parse

from pipeline.prowlarr import safe_prowlarr_download_uri


BAD_TITLE_PATTERN = re.compile(r"\b(cam|hdcam|ts|tc|scr|sample)\b", re.IGNORECASE)
UNCENSORED_PATTERN = re.compile(r"(?<![a-z0-9])(uncensored|uc)(?![a-z0-9])|无码|無碼", re.IGNORECASE)
CHINESE_SUBTITLE_PATTERN = re.compile(r"(?<![a-z0-9])(chs|cht|chinese)(?![a-z0-9])|中文|中字|简中|簡中|繁中", re.IGNORECASE)
QUERY_TOKEN_PATTERN = re.compile(r"[\u4e00-\u9fff]+|[a-z0-9]+", re.IGNORECASE)
LATIN_TOKEN_PATTERN = re.compile(r"^[a-z0-9]+$", re.IGNORECASE)
STOP_WORDS = {"a", "an", "and", "for", "in", "of", "or", "the", "to", "with"}
UNCENSORED_BONUS = 80000
CHINESE_SUBTITLE_BONUS = 40000
SUKEBEI_BONUS = 200000
QUERY_MATCH_BONUS = 1000000
QUERY_PHRASE_BONUS = 200000
EXACT_TITLE_BONUS = 300000
TITLE_EXTRA_CHAR_PENALTY = 3000
MAX_TITLE_EXTRA_PENALTY = 120000
SEEDER_BONUS_SCALE = 1000
ZERO_SEED_DHT_PENALTY = 50000
PROWLARR_PRIORITY_BONUS_SCALE = 5000
ZERO_SEED_DHT_INDEXER_TOKENS = (
    "0magnet",
    "1337x",
    "knaben",
    "magnetdownload",
    "the pirate bay",
    "torrentkitty",
    "torrentproject",
)


class ResourceSelector:
    def __init__(self, indexer_priorities=None):
        self.indexer_priorities = dict(indexer_priorities or {})

    def select_best(self, candidates, query=None):
        return self.select_rank(candidates, rank=1, query=query)

    def select_rank(self, candidates, rank, query=None):
        if rank < 1:
            raise RuntimeError("resource rank out of range: %s" % rank)
        ranked = self.select_ranked(candidates, query=query)
        if rank > len(ranked):
            raise RuntimeError("resource rank out of range: %s" % rank)
        return ranked[rank - 1]

    def select_ranked(self, candidates, query=None):
        query_text = str(query or "")
        query_tokens = self._query_tokens(query)
        scored_by_key = {}
        for item in candidates:
            candidate = self._normalize(item, query=query_text, query_tokens=query_tokens)
            if candidate is None:
                continue
            key = self._dedupe_key(candidate)
            if key not in scored_by_key or self._should_replace_duplicate(scored_by_key[key], candidate):
                scored_by_key[key] = candidate

        scored = list(scored_by_key.values())
        if not scored:
            raise RuntimeError("no acceptable resource")

        scored.sort(key=lambda item: item["score"], reverse=True)
        for index, item in enumerate(scored, start=1):
            item["rank"] = index
        return scored

    def select_ranked_limited(self, candidates, query=None, limit=None):
        ranked = self.select_ranked(candidates, query=query)
        if limit is None:
            return ranked
        limit = int(limit)
        if limit < 1 or len(ranked) <= limit:
            return ranked
        selected = self._limit_preserving_sukebei(ranked, limit)
        for index, item in enumerate(selected, start=1):
            item["rank"] = index
        return selected

    def _normalize(self, item, query=None, query_tokens=None):
        title = str(item.get("title") or item.get("titleSlug") or "")
        if not self._title_matches_query(title, query_tokens or []):
            return None

        seeders = int(item.get("seeders") or 0)
        is_sukebei = self.is_sukebei_item(item)
        zero_seed_dht = seeders <= 0 and self.is_zero_seed_dht_item(item)
        if seeders <= 0 and not is_sukebei and not zero_seed_dht:
            return None

        magnet_url = item.get("magnetUrl")
        download_uri = magnet_url if self._is_magnet(magnet_url) else None
        if not download_uri and item.get("infoHash"):
            download_uri = "magnet:?xt=urn:btih:%s" % item["infoHash"]
        if not download_uri:
            download_uri = safe_prowlarr_download_uri(item.get("downloadUrl"))
        if not download_uri:
            return None

        penalty = 1000000000 if BAD_TITLE_PATTERN.search(title) else 0
        match_bonus = self._match_bonus(title, query, query_tokens or [])
        quality_bonus = self._quality_bonus(title)
        preference_bonus = self._preference_bonus(title) + self._indexer_bonus(item)
        if zero_seed_dht:
            preference_bonus -= ZERO_SEED_DHT_PENALTY
        score = match_bonus + self._seeder_bonus(seeders) + quality_bonus + preference_bonus - penalty
        result = dict(item)
        result["download_uri"] = download_uri
        result["score"] = score
        return result

    def _match_bonus(self, title, query, query_tokens):
        if not query_tokens:
            return 0
        compact_title = self._compact_text(title)
        compact_query = self._compact_text(query)
        if not compact_query:
            return QUERY_MATCH_BONUS

        bonus = QUERY_MATCH_BONUS
        if compact_title == compact_query:
            bonus += EXACT_TITLE_BONUS
        if compact_query in compact_title:
            bonus += QUERY_PHRASE_BONUS
            extra_length = max(0, len(compact_title) - len(compact_query))
            bonus -= min(extra_length * TITLE_EXTRA_CHAR_PENALTY, MAX_TITLE_EXTRA_PENALTY)
        return bonus

    def _seeder_bonus(self, seeders):
        seeders = max(0, int(seeders or 0))
        return int(math.log2(seeders + 1) * SEEDER_BONUS_SCALE)

    def _quality_bonus(self, title):
        lowered = title.lower()
        if "2160p" in lowered or "4k" in lowered:
            return 30000
        if "1080p" in lowered:
            return 20000
        if "720p" in lowered:
            return 10000
        return 0

    def _preference_bonus(self, title):
        bonus = 0
        if UNCENSORED_PATTERN.search(title):
            bonus += UNCENSORED_BONUS
        if CHINESE_SUBTITLE_PATTERN.search(title):
            bonus += CHINESE_SUBTITLE_BONUS
        return bonus

    def _indexer_bonus(self, item):
        priority = self._indexer_priority(item)
        if priority is not None:
            return max(0, 50 - int(priority)) * PROWLARR_PRIORITY_BONUS_SCALE
        if self.is_sukebei_item(item):
            return SUKEBEI_BONUS
        return 0

    def _indexer_priority(self, item):
        indexer_id = item.get("indexerId") or item.get("indexer_id")
        try:
            return self.indexer_priorities.get(int(indexer_id))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def is_sukebei_item(item):
        values = [
            item.get("name"),
            item.get("indexer"),
            item.get("indexerName"),
            item.get("site"),
            item.get("tracker"),
            item.get("downloadUrl"),
            item.get("guid"),
            item.get("infoUrl"),
            item.get("details"),
        ]
        text = " ".join(str(value or "") for value in values).casefold()
        return "sukebei" in text or "sukebei.nyaa.si" in text

    @staticmethod
    def is_anime_specialized_item(item):
        values = [
            item.get("name"),
            item.get("indexer"),
            item.get("indexerName"),
            item.get("site"),
            item.get("tracker"),
            item.get("downloadUrl"),
            item.get("guid"),
            item.get("infoUrl"),
            item.get("details"),
        ]
        text = " ".join(str(value or "") for value in values).casefold()
        if "sukebei" in text:
            return False
        return (
            "acg.rip" in text
            or "bangumi moe" in text
            or "bangumi.moe" in text
            or "mikan" in text
            or "mikanani" in text
            or "nyaa.si" in text
            or "nyaa" in text
        )

    @staticmethod
    def is_zero_seed_dht_item(item):
        values = [
            item.get("name"),
            item.get("indexer"),
            item.get("indexerName"),
            item.get("site"),
            item.get("tracker"),
            item.get("downloadUrl"),
            item.get("guid"),
            item.get("infoUrl"),
            item.get("details"),
        ]
        text = " ".join(str(value or "") for value in values).casefold()
        return any(token in text for token in ZERO_SEED_DHT_INDEXER_TOKENS)

    def _limit_preserving_sukebei(self, ranked, limit):
        sukebei = [item for item in ranked if self.is_sukebei_item(item)]
        if not sukebei:
            return ranked[:limit]
        selected = list(sukebei)
        selected_ids = {self._dedupe_key(item) for item in selected}
        for item in ranked:
            if len(selected) >= limit:
                break
            key = self._dedupe_key(item)
            if key in selected_ids:
                continue
            selected.append(item)
            selected_ids.add(key)
        selected.sort(key=lambda item: item["score"], reverse=True)
        return selected

    def _should_replace_duplicate(self, existing, candidate):
        existing_is_sukebei = self.is_sukebei_item(existing)
        candidate_is_sukebei = self.is_sukebei_item(candidate)
        if candidate_is_sukebei and not existing_is_sukebei:
            return True
        if existing_is_sukebei and not candidate_is_sukebei:
            return False
        return candidate["score"] > existing["score"]

    def _is_magnet(self, value):
        return bool(value and urllib.parse.urlsplit(value).scheme.lower() == "magnet")

    def _dedupe_key(self, item):
        info_hash = str(item.get("infoHash") or "").strip().lower()
        if info_hash:
            return "hash:%s" % info_hash
        download_uri = item.get("download_uri")
        if self._is_magnet(download_uri):
            params = urllib.parse.parse_qs(urllib.parse.urlsplit(download_uri).query)
            for xt in params.get("xt") or []:
                prefix = "urn:btih:"
                if xt.lower().startswith(prefix):
                    return "hash:%s" % xt[len(prefix) :].lower()
        return "uri:%s" % download_uri

    def _query_tokens(self, query):
        tokens = []
        for token in QUERY_TOKEN_PATTERN.findall(str(query or "").lower()):
            if LATIN_TOKEN_PATTERN.match(token) and (len(token) <= 1 or token in STOP_WORDS):
                continue
            tokens.append(token)
        return tokens

    def _title_matches_query(self, title, query_tokens):
        if not query_tokens:
            return True
        compact_title = self._compact_text(title)
        return all(token in compact_title for token in query_tokens)

    def _compact_text(self, value):
        return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", str(value or "").lower())
