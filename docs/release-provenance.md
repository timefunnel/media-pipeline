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

Protected release branches must reject force pushes and deletion. Direct
updates to `main` are allowed only after the user explicitly authorizes a
release and the exact release tag is created. Before any history migration, first push an
`archive/...` tag for every legacy tip. A `forced-update` or shallow fetch is a
release blocker until the ancestry and semantic differences are audited.

## Branch Protection

The active GitHub ruleset targets `main`, blocks branch deletion and force
pushes, and permits direct release updates after explicit user authorization.
The provenance script and annotated release tags remain mandatory release gates
alongside that server-side protection.
