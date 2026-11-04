// Small HTML polish applied to every docs/blog page at build time:
// heading anchors, scrollable table wrappers, and external-link markers.
const isEl = (n, tag) => n?.type === 'element' && (!tag || n.tagName === tag);

export function rehypeProse() {
  return (tree) => {
    const visit = (node) => {
      if (!node.children) return;
      node.children = node.children.map((child) => {
        if (isEl(child, 'table')) {
          return { type: 'element', tagName: 'div', properties: { className: ['table-wrap'] }, children: [child] };
        }
        if (isEl(child, 'td') && child.children.length === 1 && child.children[0].type === 'text') {
          const v = child.children[0].value.trim();
          const kind = /^(yes|✓|✔|supported)$/i.test(v) ? 'yes' : /^(no|✗|✘|not supported|unsupported)$/i.test(v) ? 'no' : /^(partial|optional|limited)$/i.test(v) ? 'partial' : null;
          if (kind) child.children = [{ type: 'element', tagName: 'span', properties: { className: ['cell-badge', `cell-${kind}`] }, children: [{ type: 'text', value: v === '✓' || v === '✔' ? 'Yes' : v === '✗' || v === '✘' ? 'No' : v }] }];
        }
        if (isEl(child) && /^h[234]$/.test(child.tagName) && child.properties?.id) {
          child.children.push({
            type: 'element',
            tagName: 'a',
            properties: { className: ['anchor'], href: `#${child.properties.id}`, ariaLabel: 'Link to this section', dataAnchor: '' },
            children: [],
          });
        }
        if (isEl(child, 'a') && /^https?:\/\//.test(String(child.properties?.href ?? '')) && !String(child.properties.href).includes('sockudo.io')) {
          child.properties.target = '_blank';
          child.properties.rel = 'noreferrer';
          child.properties.className = [...(child.properties.className ?? []), 'ext'];
        }
        visit(child);
        return child;
      });
    };
    visit(tree);
  };
}
