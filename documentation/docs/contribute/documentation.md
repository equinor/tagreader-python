---
title: Documentation
sidebar_position: 5
---
# Documentation

This site was generated from the contents of your `documentation` folder using [Docusaurus,](https://docusaurus.io/) and we host it on GitHub Pages.

## How it works

From Docusaurus own documentation:
> Docusaurus is a static-site generator. It builds a single-page application with fast client-side navigation, leveraging the full power of React to make your site interactive. It provides out-of-the-box documentation features but can be used to create any kind of site (personal website, product, blog, marketing landing pages, etc).

While Docusaurus is rich on features, we use it mostly to host markdown pages. The main bulk of the documentation is located in `documentation/docs`.

## Publishing

The reusable [`docs.yaml`](https://github.com/equinor/tagreader-python/blob/main/.github/workflows/docs.yaml)
workflow builds and publishes this site. It is called on pushes to `main` by
[`on-push-main-branch.yml`](https://github.com/equinor/tagreader-python/blob/main/.github/workflows/on-push-main-branch.yml)
and can also be started manually.

It installs the dependencies pinned in `documentation/yarn.lock`, checks
TypeScript, builds the site with Node.js 24, and pushes `documentation/build`
to the `gh-pages` branch. It does not download a separately generated changelog.

## Local checks

From the `documentation` directory, using Node.js 24 and Yarn 1.22.19:

```shell
yarn install --frozen-lockfile
yarn typecheck
yarn build
yarn start
```

Check changed pages in the local preview as well as running the production build.

## Initial settings

When deployed to GitHub Pages, you do need to configure your site under the settings. Pick the gh-pages branch and select either a private url or a public one. It will show you the site’s url, which should now contain your generated documentation site.

## Assets

All assets files are places under `documentation/static`
