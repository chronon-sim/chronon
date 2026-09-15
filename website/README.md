# Website

This website is built using [Docusaurus](https://docusaurus.io/), a modern static website generator.

## Installation

Use Node.js 20 or newer (CI uses Node.js 22) and npm. The committed
`package-lock.json` defines the dependency tree used locally and in CI.

```bash
npm ci
```

## Local Development

```bash
npm start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

## Build

```bash
npm run build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

## Dependency maintenance

Validate dependency updates with:

```bash
npm ci
npm audit --audit-level=low
npm run typecheck
npm run test:dependencies
npm run build
```

Three scoped overrides keep Docusaurus 3.10.1's dependency tree on patched
versions:

- `copy-webpack-plugin` and `css-minimizer-webpack-plugin` use
  `serialize-javascript` 7.1.1 or newer within major version 7. Version 7 requires
  Node.js 20+, matching this website's existing requirement.
- `sockjs` uses `uuid` 11.1.1 or newer within major version 11, which retains the
  CommonJS `v4()` API used to create SockJS connection IDs.

The dependency tests exercise serialization through both webpack plugins and
SockJS message exchanges over XHR and WebSocket. Revisit these overrides when the
upstream consumers accept patched versions themselves. See
[Chronon #130](https://github.com/chronon-sim/chronon/issues/130).

## Deployment

The repository's `Deploy Docs` GitHub Actions workflow builds the site and
publishes it to Cloudflare Pages. Pull requests receive preview deployments.
