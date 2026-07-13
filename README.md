# Media Pipeline

Telegram Bot driven media ingestion pipeline:

```text
Telegram query or magnet
-> Prowlarr search and candidate selection
-> 115 Open offline task
-> OpenList 115 mount
-> MediaStationGo cloud root scan and scrape
-> optional OpenList cleanup and subtitle matching
```

PanSou can be enabled as an optional netdisk search supplement:

```text
Telegram search result
-> Netdisk search button
-> PanSou /api/search with cloud_types=["115"]
-> 115 share candidates
-> existing 115 share receive path
```

115 share links are handled as a separate ingestion path:

```text
Telegram 115 share link
-> choose target library
-> 115 web share receive with state.db P115 cookie or P115_COOKIE fallback
-> OpenList 115 mount
-> MediaStationGo cloud root scan and scrape
```

Use `/p115_cookie` in the Telegram Bot to check or refresh the 115 web cookie by QR login. The saved cookie is stored in the Bot state database and takes effect without restarting the container. `P115_COOKIE` remains only a startup fallback.

The compose file includes a local PanSou API service bound to `127.0.0.1:8888`. Keep `PANSOU_TOKEN` empty unless PanSou auth is enabled; the Bot only calls it from the host network and requests `cloud_types=["115"]`.

## Documentation

- Fresh deployment guide: [docs/deployment-guide.md](docs/deployment-guide.md)
- Current design and operating state: [docs/project-design-and-progress-2026-07-02.md](docs/project-design-and-progress-2026-07-02.md)
- Refactor notes: [docs/optimization-plan.md](docs/optimization-plan.md)

## Security

Do not commit `.env`, `.env.*`, `backups/`, database files, subtitle cache, OpenList data, Prowlarr config, or MediaStationGo data. The tracked `.env.example` is a template only and uses placeholders for deployment-specific ids and secrets.
