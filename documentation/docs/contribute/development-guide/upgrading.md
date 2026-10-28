# Upgrading

## Dependencies

Runtime dependencies and development dependency groups are declared in
`pyproject.toml`. Resolved versions are committed in `uv.lock`.

For an intentional update of a specific dependency:

```shell
uv lock --upgrade-package pandas
uv sync --locked
```

Review both files and run the [tests](testing.md) before merging. Avoid upgrading
unrelated packages as part of a maintenance fix. If a dependency constraint or
the project version changes in `pyproject.toml`, refresh and check the lockfile:

```shell
uv lock
uv lock --check
```

CI uses `uv sync --locked`, so it fails rather than silently rewriting a stale
lockfile. Release Please is configured to update the project's own lockfile
version during a release; dependency changes still require `uv lock`.

## Documentation dependencies

The website has a separate `documentation/package.json` and `documentation/yarn.lock`.
Use Node.js 24 and Yarn 1.22.19. After an intentional dependency change, run
`yarn install` from `documentation` and commit the updated lockfile. Verify with:

```shell
yarn install --frozen-lockfile
yarn typecheck
yarn build
```