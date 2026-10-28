# Website

This website uses [Docusaurus 3](https://docusaurus.io/) and is published at
https://equinor.github.io/tagreader-python/.

Run the following commands from the `documentation` directory. Use Node.js 24
and Yarn Classic 1.22.19, matching the documentation workflow.

### Installation

```shell
npm install --global yarn@1.22.19
yarn install --frozen-lockfile
```

### Local Development

```shell
yarn start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

### Build

```shell
yarn typecheck
yarn build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

### Deployment

The GitHub Actions workflow in `.github/workflows/docs.yaml` builds and pushes
the site to `gh-pages`. It is called by the main-branch push workflow and can
also be started manually from the Actions tab. GitHub Pages must serve the
`gh-pages` branch from its root directory.
