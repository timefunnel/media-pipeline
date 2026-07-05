# Media Pipeline Optimization Plan

## Scope

This plan tracks the remaining optimization work for the MediaPipeline bot after the current review.

All implementation work must follow these rules:

- Keep each change small and independently deployable.
- Do not change business behavior during mechanical extraction steps.
- Run local tests and remote container tests before committing.
- Commit each completed step separately.
- Deploy the bot after each committed step and verify `MEDIA_PIPELINE_REVISION`.
- Do not request OpenList or 115 unless the step explicitly requires it.
- Prefer OpenList Meta Hide over deleting source files.

## Completed Baseline

- Task state machine is available in `app/pipeline/task_state.py`.
- Search source timing and result statistics are available in `app/pipeline/search_stats.py`.
- Library routing configuration has been externalized.
- Test suites have been split into bot and service domains.
- Search helpers have been extracted from `bot.py` into `app/pipeline/search.py`.

## Planned Steps

### 1. Extract Dedupe Logic

Goal:
Move dedupe storage, identity normalization, duplicate lookup, and OpenList dedupe entry helpers out of `bot.py`.

Constraints:
- Keep existing imports from `pipeline.bot` compatible during the extraction.
- Do not change duplicate matching rules.
- Do not change OpenList refresh behavior.

Validation:
- `python -m py_compile app/pipeline/*.py tests/*.py`
- `python -m unittest tests.pipeline_test_bot`
- `python -m unittest tests.pipeline_test_services`
- `python -m unittest tests.test_pipeline_core`
- Remote container `tests.test_pipeline_core`

### 2. Extract Migration Flow Helpers

Goal:
Separate media migration orchestration and formatting helpers from Telegram callback handling.

Constraints:
- Keep MSG database migration semantics unchanged.
- Keep dedupe index migration coupled to library/path migration.
- Keep confirmation buttons and cancellation behavior unchanged.

Validation:
- Existing migration tests must pass.
- Add focused tests only if extraction exposes uncovered behavior.

### 3. Extract Telegram UI Formatting

Goal:
Move reply markup builders and message formatting functions into a UI module.

Constraints:
- No text or button behavior changes unless explicitly requested.
- Keep callback payload formats stable.

Validation:
- Existing bot interaction tests must pass.
- Verify `/help`, `/tasks`, migration, dedupe refresh, search pagination, and task status paths.

### 4. Tighten Task State Usage

Goal:
Make task button visibility, retry eligibility, cancellation eligibility, and syncing display depend on `TASK_STATE`.

Constraints:
- Do not hide actionable buttons for active tasks.
- Do not show refresh/cancel buttons for final tasks.
- Avoid ad hoc status checks outside the state machine.

Validation:
- Add or update tests for active, cancelled, failed, success, and sync-running states.

### 5. Standardize Hide-Only Cleanup

Goal:
Remove remaining source-file deletion paths from normal media cleanup and keep Meta Hide as the default cleanup mechanism.

Constraints:
- Do not delete 115/OpenList source files in the normal bot flow.
- Keep subtitles visible.
- Hide extras, PV, small non-main videos, and non-library junk through OpenList Meta Hide.

Validation:
- Cleanup tests must prove hide calls are made instead of delete calls.
- Use normal-user visibility rules when validating against OpenList manually.

### 6. Data-Driven Search Tuning

Goal:
Use recorded search source stats to tune Prowlarr indexer priority, timeout, concurrency, and category/tag boundaries.

Constraints:
- Do not hard-code more source-specific behavior in Telegram interaction code.
- Keep Sukebei/fanhao preservation semantics intact.
- Keep search response under the target latency when upstreams are healthy.

Validation:
- Unit tests for timeout, partial failure, source stats, and result preservation.
- Manual bot search smoke test only if needed.

### 7. Emby Compatibility Regression Coverage

Goal:
Add targeted regression coverage for subtitle injection and playback progress behavior used by Infuse/Vidhub.

Constraints:
- Do not break native MSG web playback.
- Do not force subtitle conversion unless required by the target client path.
- Keep runtime and resume field patching explicit and testable.

Validation:
- Unit tests for subtitle stream injection, external subtitle discovery, PlaybackInfo patching, and resume item patching.
- Manual client verification can be done after code tests when a client behavior changes.
