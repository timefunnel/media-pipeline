# Media Pipeline

Telegram Bot driven media ingestion pipeline:

```text
Telegram query or magnet
-> Prowlarr search and candidate selection
-> 115 Open offline task
-> OpenList 115 mount
-> MediaStationGo cloud root scan and scrape
-> optional cleanup, artwork repair, subtitle matching and Emby-compatible proxy
```

## Documentation

- Fresh deployment guide: [docs/deployment-guide.md](docs/deployment-guide.md)
- Current design and operating state: [docs/project-design-and-progress-2026-07-02.md](docs/project-design-and-progress-2026-07-02.md)
- Refactor notes: [docs/optimization-plan.md](docs/optimization-plan.md)

## Security

Do not commit `.env`, `.env.*`, `backups/`, database files, subtitle cache, OpenList data, Prowlarr config, or MediaStationGo data. The tracked `.env.example` is a template only and uses placeholders for deployment-specific ids and secrets.
