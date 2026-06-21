import { RootProvider } from 'fumadocs-ui/provider/next';
import type { Metadata, Viewport } from 'next';
import './global.css';
import { Inter } from 'next/font/google';
import { DatabuddyAnalytics } from '@/components/databuddy-analytics';
import { siteUrl } from '@/lib/shared';

const inter = Inter({
  subsets: ['latin'],
  display: 'swap',
});

export const metadata: Metadata = {
  metadataBase: new URL(siteUrl),
  title: {
    default: 'Sockudo Docs',
    template: '%s | Sockudo Docs',
  },
  description:
    'Professional documentation for Sockudo, the Rust realtime server with Pusher compatibility, Protocol V2, recovery, history, push, and official SDKs.',
  applicationName: 'Sockudo Docs',
  icons: {
    icon: '/favicon.svg',
    apple: '/apple-touch-icon.png',
  },
  openGraph: {
    type: 'website',
    siteName: 'Sockudo Docs',
    title: 'Sockudo Docs',
    description:
      'Build and operate self-hosted realtime infrastructure with Sockudo.',
    images: ['/logo.svg'],
  },
};

export const viewport: Viewport = {
  themeColor: [
    { media: '(prefers-color-scheme: light)', color: '#fefcfb' },
    { media: '(prefers-color-scheme: dark)', color: '#0a131a' },
  ],
};

export default function Layout({ children }: LayoutProps<'/'>) {
  return (
    <html lang="en" className={inter.className} suppressHydrationWarning>
      <body className="flex flex-col min-h-screen">
        <RootProvider>{children}</RootProvider>
        <DatabuddyAnalytics />
      </body>
    </html>
  );
}
