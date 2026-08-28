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
-> 115 web share receive with the Cookie managed by MediaStationGo's cloud115 storage config
-> OpenList 115 mount
-> MediaStationGo cloud root scan and scrape
```

Maintain the 115 web Cookie in the MediaStationGo admin UI under `外部存储 -> 115网盘`, either by entering it or using 115 App QR login. The Bot reads that decrypted admin storage config when receiving a share; it does not persist or update a second Cookie in its own state database. `/p115_cookie` only points to the MSG management page for older clients.

The compose file includes a local PanSou API service bound to `127.0.0.1:8888`. Keep `PANSOU_TOKEN` empty unless PanSou auth is enabled; the Bot only calls it from the host network and requests `cloud_types=["115"]`.

## Documentation

- Fresh deployment guide: [docs/deployment-guide.md](docs/deployment-guide.md)
- Current design and operating state: [docs/project-design-and-progress-2026-07-02.md](docs/project-design-and-progress-2026-07-02.md)
- Refactor notes: [docs/optimization-plan.md](docs/optimization-plan.md)

## Security

Do not commit `.env`, `.env.*`, `backups/`, database files, subtitle cache, OpenList data, Prowlarr config, or MediaStationGo data. The tracked `.env.example` is a template only and uses placeholders for deployment-specific ids and secrets.
