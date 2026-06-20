import defaultMdxComponents from 'fumadocs-ui/mdx';
import type { ComponentProps, ReactNode } from 'react';
import type { MDXComponents } from 'mdx/types';
import { MermaidDiagram } from './mermaid-diagram';

type PreProps = ComponentProps<'pre'>;

const mermaidDirectives = [
  'sequenceDiagram',
  'flowchart',
  'graph',
  'classDiagram',
  'stateDiagram',
  'stateDiagram-v2',
  'erDiagram',
  'journey',
  'gantt',
  'pie',
  'quadrantChart',
  'requirementDiagram',
  'gitGraph',
  'mindmap',
  'timeline',
  'zenuml',
  'sankey-beta',
  'xychart-beta',
  'block-beta',
  'packet-beta',
  'architecture-beta',
];

function textFromNode(node: ReactNode): string {
  if (node == null || typeof node === 'boolean') return '';
  if (typeof node === 'string' || typeof node === 'number') return String(node);
  if (Array.isArray(node)) return node.map(textFromNode).join('');

  if (typeof node === 'object' && 'props' in node) {
    const props = node.props as { children?: ReactNode };
    return textFromNode(props.children);
  }

  return '';
}

function isMermaidChart(code: string) {
  const firstLine = code.trimStart().split('\n', 1)[0]?.trim() ?? '';
  return mermaidDirectives.some((directive) => firstLine.startsWith(directive));
}

function MermaidAwarePre(props: PreProps) {
  const code = textFromNode(props.children);

  if (isMermaidChart(code)) {
    return <MermaidDiagram chart={code.trim()} />;
  }

  const Pre = defaultMdxComponents.pre;
  return <Pre {...props} />;
}

export function getMDXComponents(components?: MDXComponents) {
  return {
    ...defaultMdxComponents,
    pre: MermaidAwarePre,
    ...components,
  } satisfies MDXComponents;
}

export const useMDXComponents = getMDXComponents;

declare global {
  type MDXProvidedComponents = ReturnType<typeof getMDXComponents>;
}
