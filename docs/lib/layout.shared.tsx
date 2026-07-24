import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { BookOpen, Code2, Newspaper, Server } from 'lucide-react';
import Image from 'next/image';
import { appName, gitConfig } from './shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span className="brand-nav">
          <Image src="/sockudo-logo/sockudo-icon-color.svg" alt="" width={28} height={28} />
          <span>{appName}</span>
        </span>
      ),
      transparentMode: 'none',
    },
    links: [
      {
        type: 'main',
        text: 'Docs',
        url: '/docs',
        icon: <BookOpen className="size-4" />,
        active: 'nested-url',
      },
      {
        type: 'main',
        text: 'SDKs',
        url: '/docs/server-sdks',
        icon: <Server className="size-4" />,
        active: 'nested-url',
      },
      {
        type: 'main',
        text: 'Blog',
        url: '/blog',
        icon: <Newspaper className="size-4" />,
        active: 'nested-url',
      },
      {
        type: 'icon',
        text: 'GitHub',
        label: 'GitHub repository',
        url: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
        icon: <Code2 className="size-4" />,
        external: true,
      },
    ],
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
  };
}
