// @ts-check
import { defineEcConfig } from 'astro-expressive-code';

export default defineEcConfig({
  themes: ['github-dark-default', 'github-light-default'],
  themeCssSelector: (theme) => (theme.type === 'dark' ? '[data-theme="dark"]' : '[data-theme="light"]'),
  useDarkModeMediaQuery: false,
  styleOverrides: {
    borderRadius: '14px',
    borderColor: 'var(--border)',
    codeFontFamily: "'Geist Mono Variable', ui-monospace, SFMono-Regular, Menlo, monospace",
    codeFontSize: '0.85rem',
    uiFontFamily: "'Geist Variable', ui-sans-serif, system-ui, sans-serif",
    frames: {
      shadowColor: 'transparent',
      editorActiveTabIndicatorTopColor: '#a855f7',
      editorTabBarBackground: 'var(--code-chrome)',
      terminalTitlebarBackground: 'var(--code-chrome)',
      frameBoxShadowCssValue: 'none',
    },
  },
});
