import DOMPurify from 'dompurify'
import { marked } from 'marked'

marked.use({
  gfm: true,
  breaks: true,
})

const markdownCache = new Map<string, string>()
const maxCacheEntries = 200

function cacheSet(source: string, html: string) {
  markdownCache.set(source, html)
  if (markdownCache.size <= maxCacheEntries) return
  const oldest = markdownCache.keys().next().value
  if (oldest) markdownCache.delete(oldest)
}

export function renderMarkdown(source: string) {
  if (!source) return ''
  const cached = markdownCache.get(source)
  if (cached) return cached

  const rawHtml = marked.parse(source, {
    async: false,
  })
  const cleanHtml = DOMPurify.sanitize(rawHtml, {
    USE_PROFILES: { html: true },
    ADD_ATTR: ['target', 'rel'],
  })
  cacheSet(source, cleanHtml)
  return cleanHtml
}
