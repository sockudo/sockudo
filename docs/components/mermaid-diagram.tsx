'use client';

import { useEffect, useId, useState } from 'react';
import mermaid from 'mermaid';

type MermaidDiagramProps = {
  chart: string;
};

mermaid.initialize({
  startOnLoad: false,
  securityLevel: 'strict',
  theme: 'base',
  themeVariables: {
    primaryColor: '#f5f0eb',
    primaryTextColor: '#0a131a',
    primaryBorderColor: '#7938d3',
    lineColor: '#5b6570',
    secondaryColor: '#f1e9fb',
    tertiaryColor: '#ffffff',
    noteBkgColor: '#fff7df',
    noteTextColor: '#0a131a',
    noteBorderColor: '#f6b23c',
  },
});

export function MermaidDiagram({ chart }: MermaidDiagramProps) {
  const id = useId().replaceAll(':', '');
  const [svg, setSvg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function renderDiagram() {
      try {
        const result = await mermaid.render(`mermaid-${id}`, chart);
        if (!cancelled) {
          setSvg(result.svg);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setSvg(null);
          setError(err instanceof Error ? err.message : 'Unable to render Mermaid diagram.');
        }
      }
    }

    void renderDiagram();

    return () => {
      cancelled = true;
    };
  }, [chart, id]);

  return (
    <figure className="mermaid-diagram not-prose my-6 overflow-auto rounded-xl border border-fd-border bg-fd-card p-4 shadow-sm">
      {svg ? (
        <div
          className="mermaid-diagram__svg min-w-max"
          dangerouslySetInnerHTML={{ __html: svg }}
        />
      ) : error ? (
        <pre className="whitespace-pre-wrap text-sm text-fd-muted-foreground">{chart}</pre>
      ) : (
        <div className="h-40 animate-pulse rounded-lg bg-fd-muted" aria-hidden="true" />
      )}
    </figure>
  );
}
