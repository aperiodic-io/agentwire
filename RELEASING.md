# Releasing

One tag releases everything: `vX.Y.Z` on a commit of `main` that passed CI.

1. Set the version everywhere it is written, the same in each: `Cargo.toml`, `python/Cargo.toml`
   (both wheels take theirs from Cargo), `ts/package.json`, and the exporter's requirement of the Go
   module (`go/otelcol/deltapackexporter/go.mod`: `github.com/aperiodic-io/deltapack/go vX.Y.Z`).
   `scripts/versions.sh` checks they agree; CI runs it on every pull request.
2. Move CHANGELOG.md's `[Unreleased]` entries under `## [X.Y.Z]`; the release notes are that section.
3. Merge, then `git tag vX.Y.Z && git push origin vX.Y.Z`.

Before a tag, the same workflow runs as a dry run on every pull request that changes what a release
builds (and by hand, from the Actions tab): each job builds what it would publish and checks it (the
crate packaged, the image run, the binaries checksummed and run, each wheel installed and used, the
sdist built from source, the npm package packed, the formula installed by Homebrew) and publishes
nothing. A tag runs it for real.

`.github/workflows/release.yml` then publishes:

| What | Where | Set up once |
|---|---|---|
| The crate | crates.io | the `CARGO_REGISTRY_TOKEN` secret |
| The image | ghcr.io/aperiodic-io/deltapack | nothing (the workflow's token) |
| Binaries (Linux x86_64 and aarch64), the release notes | the GitHub release | nothing |
| `deltapack`, `deltapack-cli` (wheels for Linux x86_64 and aarch64; the library's sdist, which pip builds elsewhere) | PyPI | for each name, a pending trusted publisher (the projects do not exist until the first upload): this repository, workflow `release.yml`, environment `pypi`; and the `pypi` environment in this repository's settings |
| `deltapack` (the TypeScript reader) | npm | a trusted publisher for the package (this repository, workflow `release.yml`, environment `npm`; npm asks for the package to exist, so the first version is published once by hand with `npm publish --access public` in `ts/`), and the `npm` environment in this repository's settings |
| `go/vX.Y.Z`, `go/otelcol/deltapackexporter/vX.Y.Z` | tags here, which the Go proxy reads | nothing |
| `deltapack.rb`, the formula with the tag's source and its sha256 | the GitHub release | copy it over `Formula/deltapack.rb` on `main` so `brew install deltapack` installs the release |

A registry that refuses (a missing secret, a name taken) fails its own job only: the others publish.
Re-run that job once it is set up. A version published to crates.io, PyPI or npm cannot be published
again: a broken release is fixed by the next version.

macOS binaries and wheels need macOS runners, which the organisation does not allow today (their jobs
are cancelled at start). Once it does, add `{ os: macos-14, target: aarch64-apple-darwin }` to the
`binaries` matrices (ci.yml and release.yml) and `{ os: macos-14, target: aarch64 }` to `wheels`.
