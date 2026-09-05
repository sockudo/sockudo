import { RootProvider } from 'fumadocs-ui/provider/next';
import type { Metadata, Viewport } from 'next';
import './global.css';
import { Inter } from 'next/font/google';
import { DatabuddyAnalytics } from '@/components/databuddy-analytics';
import { TypeUIConsent } from '@/components/typeui-consent';
import { siteUrl } from '@/lib/shared';
import { typeUIInsightsSnippet } from '@/lib/typeui-insights';

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
    'Documentation for Sockudo, the self-hosted Rust realtime server with Pusher compatibility, opt-in Ably REST and WebSocket compatibility excluding Live Objects, Protocol V2, recovery, push, AI Transport, and official SDKs.',
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
      'Build self-hosted realtime infrastructure with Pusher compatibility and opt-in Ably REST and WebSocket compatibility excluding Live Objects.',
    images: ['/logo.svg'],
  },
};

export const viewport: Viewport = {
  themeColor: [
    { media: '(prefers-color-scheme: light)', color: '#fbfaff' },
    { media: '(prefers-color-scheme: dark)', color: '#0b0811' },
  ],
};

export default function Layout({ children }: LayoutProps<'/'>) {
  return (
    <html lang="en" className={inter.className} suppressHydrationWarning>
      <head>
        <script
          data-typeui-site-verification="tui_verify_YQzmN7rICAlhxMTswui_-BKCggnpYdX-"
          dangerouslySetInnerHTML={{ __html: typeUIInsightsSnippet }}
        />
      </head>
      <body className="flex flex-col min-h-screen">
        <a className="skip-link" href="#main-content">
          Skip to main content
        </a>
        <RootProvider>{children}</RootProvider>
        <DatabuddyAnalytics />
        <TypeUIConsent />
      </body>
    </html>
  );
}
