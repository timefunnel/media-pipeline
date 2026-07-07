import json
import re
import urllib.error
import urllib.request


DEFAULT_LLM_BASE_URL = "https://api.deepseek.com/v1"
DEFAULT_LLM_MODEL = "deepseek-v4-flash"
DEFAULT_LLM_TIMEOUT_SECONDS = 4
DEFAULT_LLM_SEARCH_RERANK_LIMIT = 40


class LlmTransport:
    def request(self, url, payload, headers=None, timeout=None):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=body,
            headers=headers or {},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", "replace")
            raise RuntimeError("LLM API failed: HTTP %s %s" % (exc.code, raw[:500])) from exc
        return json.loads(raw)


class SearchRerankClient:
    def __init__(
        self,
        base_url=DEFAULT_LLM_BASE_URL,
        api_key="",
        model=DEFAULT_LLM_MODEL,
        timeout=DEFAULT_LLM_TIMEOUT_SECONDS,
        thinking_disabled=True,
        transport=None,
    ):
        self.base_url = str(base_url or DEFAULT_LLM_BASE_URL).rstrip("/")
        self.api_key = str(api_key or "").strip()
        self.model = str(model or DEFAULT_LLM_MODEL).strip()
        self.timeout = int(timeout or DEFAULT_LLM_TIMEOUT_SECONDS)
        self.thinking_disabled = bool(thinking_disabled)
        self.transport = transport or LlmTransport()

    def rerank_search_candidates(self, query, category, candidates, max_candidates=None):
        candidates = [dict(candidate) for candidate in candidates or []]
        if len(candidates) <= 1:
            return self._ranked_copy(candidates)
        if not self.api_key:
            raise RuntimeError("LLM_API_KEY missing")

        max_candidates = int(max_candidates or len(candidates))
        if max_candidates <= 1:
            return self._ranked_copy(candidates)
        subset = candidates[:max_candidates]
        tail = candidates[max_candidates:]

        request_candidates = [self._candidate_payload(index, candidate) for index, candidate in enumerate(subset, start=1)]
        payload = self._request_payload(query, category, request_candidates)
        response = self.transport.request(
            self.base_url + "/chat/completions",
            payload,
            headers={
                "Authorization": "Bearer %s" % self.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=self.timeout,
        )
        content = self._message_content(response)
        result = self._parse_result(content)
        selected_ids = self._validate_selected_ids(result, {item["id"] for item in request_candidates})

        by_id = {item["id"]: candidate for item, candidate in zip(request_candidates, subset)}
        ordered = [by_id[item_id] for item_id in selected_ids]
        selected = set(selected_ids)
        ordered.extend(candidate for item, candidate in zip(request_candidates, subset) if item["id"] not in selected)
        ordered.extend(tail)
        return self._ranked_copy(ordered)

    def rank_subtitle_candidates(self, media, query, candidates, max_candidates=None):
        candidates = [dict(candidate) for candidate in candidates or []]
        if len(candidates) <= 1:
            return self._ranked_copy(candidates)
        if not self.api_key:
            raise RuntimeError("LLM_API_KEY missing")

        max_candidates = int(max_candidates or len(candidates))
        if max_candidates <= 1:
            return self._ranked_copy(candidates)
        subset = candidates[:max_candidates]
        tail = candidates[max_candidates:]

        request_candidates = [self._subtitle_candidate_payload(index, candidate) for index, candidate in enumerate(subset, start=1)]
        payload = self._subtitle_request_payload(media, query, request_candidates)
        response = self.transport.request(
            self.base_url + "/chat/completions",
            payload,
            headers={
                "Authorization": "Bearer %s" % self.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=self.timeout,
        )
        content = self._message_content(response)
        result = self._parse_result(content)
        selected_ids = self._validate_selected_ids(result, {item["id"] for item in request_candidates})

        by_id = {item["id"]: candidate for item, candidate in zip(request_candidates, subset)}
        reasons = result.get("reasons") if isinstance(result.get("reasons"), dict) else {}
        confidence = result.get("confidence")
        best_id = result.get("best_id")
        ordered = []
        for item_id in selected_ids:
            candidate = dict(by_id[item_id])
            reason = reasons.get(item_id)
            if isinstance(reason, str) and reason.strip():
                candidate["llm_reason"] = reason.strip()[:120]
            candidate["llm_confidence"] = confidence
            candidate["llm_best"] = item_id == best_id
            ordered.append(candidate)
        selected = set(selected_ids)
        ordered.extend(candidate for item, candidate in zip(request_candidates, subset) if item["id"] not in selected)
        ordered.extend(tail)
        return self._ranked_copy(ordered)

    def _request_payload(self, query, category, candidates):
        system = (
            "You are a filename search result reranker. Return strict json only, no markdown. "
            "Required schema example: "
            '{"selected_ids":["c2","c1"],"best_id":"c2","confidence":0.95,"reason":"exact match"}. '
            "selected_ids must contain candidate ids ordered from best to worst. "
            "Use only ids from candidates. Exact title or main code match beats seeders. "
            "Different main codes must not outrank exact main code matches. "
            "Treat -C, CHS, CHT, Chinese subtitles as subtitle variants, not different main codes. "
            "Prefer requested subtitles and quality only after exact title/main code match. "
            "confidence must be a number from 0 to 1. reason must be short."
        )
        payload = {
            "model": self.model,
            "max_tokens": 600,
            "stream": False,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "query": str(query or ""),
                            "category": str(category or ""),
                            "candidates": candidates,
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                },
            ],
        }
        if self.thinking_disabled:
            payload["thinking"] = {"type": "disabled"}
        return payload

    def _candidate_payload(self, index, candidate):
        return {
            "id": "c%s" % index,
            "title": str(candidate.get("title") or ""),
            "indexer": str(candidate.get("indexer") or candidate.get("indexerName") or ""),
            "seeders": int(candidate.get("seeders") or 0),
            "size": int(candidate.get("size") or 0),
            "rank": int(candidate.get("rank") or index),
        }

    def _subtitle_request_payload(self, media, query, candidates):
        system = (
            "You are a Chinese subtitle candidate evaluator. Return strict json only, no markdown. "
            "Required schema example: "
            '{"selected_ids":["c2","c1"],"best_id":"c2","confidence":0.8,"reasons":{"c2":"exact code and zh-Hans"}}. '
            "selected_ids must contain candidate ids ordered from best to worst. Use only ids from candidates. "
            "Evaluate the subtitle content_sample when present. Prefer exact adult code/title match, readable Chinese text, "
            "cleaner filenames, and higher source score. Penalize empty, garbled, unrelated, or non-Chinese samples. "
            "Do not invent candidates. reasons values must be short."
        )
        payload = {
            "model": self.model,
            "max_tokens": 700,
            "stream": False,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "media": {
                                "title": str((media or {}).get("title") or ""),
                                "code": str((media or {}).get("code") or ""),
                                "media_id": str((media or {}).get("media_id") or ""),
                            },
                            "query": str(query or ""),
                            "candidates": candidates,
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                },
            ],
        }
        if self.thinking_disabled:
            payload["thinking"] = {"type": "disabled"}
        return payload

    def _subtitle_candidate_payload(self, index, candidate):
        return {
            "id": "c%s" % index,
            "provider": str(candidate.get("provider") or ""),
            "title": str(candidate.get("title") or ""),
            "filename": str(candidate.get("filename") or ""),
            "language": str(candidate.get("language") or ""),
            "query": str(candidate.get("query") or ""),
            "code": str(candidate.get("code") or ""),
            "source_score": int(candidate.get("source_score") or 0),
            "preview_char_count": int(candidate.get("preview_char_count") or 0),
            "preview_line_count": int(candidate.get("preview_line_count") or 0),
            "preview_error": str(candidate.get("preview_error") or ""),
            "content_sample": str(candidate.get("content_sample") or "")[:2500],
            "rank": int(candidate.get("rank") or index),
        }

    def _message_content(self, response):
        choices = response.get("choices") or []
        if not choices:
            raise RuntimeError("LLM response missing choices")
        message = choices[0].get("message") or {}
        content = str(message.get("content") or "").strip()
        if not content:
            raise RuntimeError("LLM response content is empty")
        return content

    def _parse_result(self, content):
        content = str(content or "").strip()
        if content.startswith("```"):
            content = re.sub(r"^```(?:json)?\s*", "", content, flags=re.IGNORECASE)
            content = re.sub(r"\s*```$", "", content)
        try:
            result = json.loads(content)
        except ValueError as exc:
            raise RuntimeError("LLM response is not valid JSON: %s" % exc) from exc
        if not isinstance(result, dict):
            raise RuntimeError("LLM response JSON must be an object")
        return result

    def _validate_selected_ids(self, result, allowed_ids):
        selected_ids = result.get("selected_ids")
        if not isinstance(selected_ids, list) or not selected_ids:
            raise RuntimeError("LLM response selected_ids must be a non-empty list")
        normalized = []
        seen = set()
        for item_id in selected_ids:
            if not isinstance(item_id, str):
                raise RuntimeError("LLM response selected_ids must contain strings")
            if item_id not in allowed_ids:
                raise RuntimeError("LLM response selected unknown candidate id: %s" % item_id)
            if item_id in seen:
                raise RuntimeError("LLM response selected duplicate candidate id: %s" % item_id)
            seen.add(item_id)
            normalized.append(item_id)

        best_id = result.get("best_id")
        if not isinstance(best_id, str) or best_id not in allowed_ids:
            raise RuntimeError("LLM response best_id is invalid")
        if best_id != normalized[0]:
            raise RuntimeError("LLM response best_id must match selected_ids[0]")

        confidence = result.get("confidence")
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
            raise RuntimeError("LLM response confidence must be numeric")
        if confidence < 0 or confidence > 1:
            raise RuntimeError("LLM response confidence out of range")
        return normalized

    def _ranked_copy(self, candidates):
        ranked = [dict(candidate) for candidate in candidates or []]
        for index, candidate in enumerate(ranked, start=1):
            candidate["rank"] = index
        return ranked
