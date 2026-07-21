import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/intro">
            Get Started
          </Link>
          <Link
            className="button button--outline button--secondary button--lg"
            style={{marginLeft: '1rem'}}
            to="/docs/api/">
            API Reference
          </Link>
        </div>
      </div>
    </header>
  );
}

type FeatureItem = {
  title: string;
  description: ReactNode;
  code?: string;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Tick-Based Simulation',
    description: (
      <>
        Define simulation units as simple state machines with a <code>tick()</code> method.
        Chronon handles scheduling, parallelization, and communication automatically.
      </>
    ),
  },
  {
    title: 'Automatic Parallelization',
    description: (
      <>
        Dependency-driven lookahead scheduling powered by <code>stdexec</code>.
        Achieve ~90+ Mcycles/sec with zero manual thread management.
      </>
    ),
  },
  {
    title: 'Type-Safe Ports',
    description: (
      <>
        Communicate between units with <code>OutPort&lt;T&gt;</code> and <code>InPort&lt;T&gt;</code>.
        Delay-based mode selection: inline (0-cycle) or SPSC queue (N-cycle).
      </>
    ),
  },
  {
    title: 'YAML-Driven Configuration',
    description: (
      <>
        Build simulations from YAML with the factory system.
        Use <code>SimulationApp</code> for a one-line entry point with full CLI support.
      </>
    ),
  },
  {
    title: 'Built-in Observability',
    description: (
      <>
        Macro-free API for counters, timeline events, and logs.
        Perfetto protobuf output for interactive timeline visualization.
      </>
    ),
  },
  {
    title: 'C++20 Modern Design',
    description: (
      <>
        Built on sender/receiver (P2300), concepts, and compile-time string formatting.
        Single include: <code>#include "chronon/Chronon.hpp"</code>.
      </>
    ),
  },
];

function Feature({title, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md padding-vert--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="Home"
      description="High-performance tick-based simulation framework for CPU microarchitecture modeling">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
