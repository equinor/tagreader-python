// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');
const simplePlantUML = require('@akebifiky/remark-simple-plantuml');
const math = require('remark-math');
const katex = require('rehype-katex');

async function createConfig() {
  const mdxMermaid = await import('mdx-mermaid')
  return {
    title: 'Tagreader',
    tagline: 'Tagreader.',
    url: 'https://awt.app.radix.equinor.com/',
    baseUrl: '/tagreader-python/',
    onBrokenLinks: 'throw',
    onBrokenMarkdownLinks: 'warn',
    favicon: 'img/favicon.png',

    // GitHub pages deployment config.
    // If you aren't using GitHub pages, you don't need these.
    organizationName: 'equinor', // Usually your GitHub org/username.
    projectName: 'tagreader', // Usually your repo name.
    deploymentBranch: 'gh-pages',

    // Even if you don't use internalization, you can use this field to set useful
    // metadata like html lang. For example, if your site is Chinese, you may want
    // to replace "en" with "zh-Hans".
    i18n: {
      defaultLocale: 'en',
      locales: ['en'],
    },

    plugins: [

    ],

    presets: [
      [
        'classic',
        /** @type {import('@docusaurus/preset-classic').Options} */
        ({
          docs: {
            sidebarPath: require.resolve('./sidebars.js'),
            editUrl:
              'https://github.com/equinor/tagreader-python/tree/main/documentation/',
              remarkPlugins: [mdxMermaid.default, simplePlantUML, math],
              rehypePlugins: [katex],
          },
          blog: false,
          theme: {
            customCss: require.resolve('./src/css/custom.css'),
          },
        }),
      ],
    ],

    themeConfig:
      /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
      ({
        navbar: {
          title: 'Tagreader',
          logo: {
            alt: 'Equinor Logo',
            src: 'img/logo.svg',
          },
          items: [
            {
              type: 'docSidebar',
              sidebarId: 'about',
              position: 'left',
              label: 'Docs',
            },
            {
              type: 'docSidebar',
              sidebarId: 'contribute',
              position: 'left',
              label: 'Contribute',
            },
            {
              href: 'https://github.com/equinor/tagreader-python',
              label: 'GitHub',
              position: 'right',
            },
          ],
        },
        footer: {
          style: 'dark',
          links: [
            {
              title: 'Docs',
              items: [
                {
                  label: 'Docs',
                  to: '/docs/about/introduction',
                },
                  {
                  label: 'Contribute',
                  to: '/docs/contribute/how-to-start-contributing',
                },
              ],
            },
            {
              title: 'More',
              items: [
                {
                  label: 'GitHub',
                  href: 'https://github.com/equinor/tagreader',
                },
                {
                  label: 'PyPi',
                  href: 'https://pypi.org/project/tagreader/',
                },
              ],
            },
          ],
          copyright: `Built with Docusaurus.`,
        },
        prism: {
          theme: lightCodeTheme,
          darkTheme: darkCodeTheme,
        },
        colorMode: {
          defaultMode: 'dark',
          disableSwitch: false,
          respectPrefersColorScheme: true,
        }
      }),
    stylesheets: [
      {
        href: 'https://cdn.jsdelivr.net/npm/katex@0.13.24/dist/katex.min.css',
        type: 'text/css',
        integrity:
          'sha384-odtC+0UGzzFL/6PNoE8rX/SPcQDXBJ+uRepguP4QkPCm2LBxH3FA3y+fKSiJ+AmM',
        crossorigin: 'anonymous',
      },
    ],
  }
}

module.exports = createConfig;
