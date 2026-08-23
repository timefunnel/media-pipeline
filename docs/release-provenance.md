# Release Provenance

Every production media-pipeline image must be built from a commit reachable
from a protected GitHub branch and exactly referenced by an annotated
`release/media-pipeline/<YYYYMMDD>-<sha7>` tag. The release record, OCI label,
runtime environment, application version, and deployed commit must agree.

Before server verification or production deployment, run:

```powershell
.\scripts\assert-release-provenance.ps1 `
  -Repository . `
  -Commit <full-sha> `
  -RemoteBranch codex/local-dev `
  -ReleaseTag release/media-pipeline/<YYYYMMDD>-<sha7>
```

The gate rejects shallow clones, dirty worktrees, commits outside the remote
branch ancestry, and tags that do not resolve to the requested commit.

Protected development branches must reject force pushes and deletion, and
require pull requests. Before any history migration, first push an
`archive/...` tag for every legacy tip. A `forced-update` or shallow fetch is a
release blocker until the ancestry and semantic differences are audited.
