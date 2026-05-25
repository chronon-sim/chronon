import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import remarkWaveDrom from './src/remark/wavedrom.mjs';

const config: Config = {
  title: 'Chronon',
  tagline: 'High-Performance Tick-Based Simulation Framework',
  favicon: 'img/favicon.ico',

  url: 'https://chronon-sim.org',
  baseUrl: '/',

  organizationName: 'chronon-sim',
  projectName: 'chronon',

  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    format: 'detect',
  },

  plugins: [
    'docusaurus-plugin-llms',
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/chronon-sim/chronon/tree/main/website/',
          remarkPlugins: [remarkWaveDrom],
          lastVersion: 'current',
          versions: {
            current: {
              label: 'Next',
              badge: false,
            },
          },
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/chronon-social-card.png',
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Chronon',
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'guideSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          type: 'docSidebar',
          sidebarId: 'apiSidebar',
          position: 'left',
          label: 'API',
        },
        {
          type: 'docsVersionDropdown',
          position: 'right',
        },
        {
          href: 'https://github.com/chronon-sim/chronon',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Getting Started', to: '/docs/intro'},
            {label: 'Architecture', to: '/docs/guides/architecture'},
            {label: 'API Reference', to: '/docs/api/'},
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/chronon-sim/chronon',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} EHTech (Beijing) Co., Ltd. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['cpp', 'cmake', 'yaml', 'bash'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
