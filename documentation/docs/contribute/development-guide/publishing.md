# Publishing

Releases are managed by
[`release-please.yml`](https://github.com/equinor/tagreader-python/blob/main/.github/workflows/release-please.yml).
Use Conventional Commit titles such as `fix: correct timestamp conversion` or
`feat: add a metadata query` when merging changes to `main`.

Release Please opens or updates a release pull request containing the version
bump and changelog. Review its changes before merging, including the package
version in `pyproject.toml` and `uv.lock`, and the release manifest under `.github`.
Check lockfile consistency with:

```shell
uv lock --check
```

When a release PR is merged, Release Please creates the GitHub release. The
workflow then calls the build and publish workflows. The publish workflow builds
distributions with `uv build` and currently uploads them with Poetry using the
repository's `PYPI_TOKEN` secret. Do not put that token in source files or logs.

To check distribution creation locally without publishing:

```shell
uv build
```

The release and publish workflows also allow manual dispatch. A manual publish
uploads the checked-out revision; it does not create a release PR or choose a new
version. Confirm the revision, version, and build results before running it.