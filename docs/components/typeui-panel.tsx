'use client';

import { useState } from 'react';
import Image from 'next/image';

const expandedPanelStyle = {
  position: 'fixed',
  left: '50%',
  bottom: '24px',
  transform: 'translateX(-50%)',
  zIndex: 2147483647,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  gap: '8px',
  padding: '10px 16px',
  background: 'rgba(0,0,0,.5)',
  color: '#fff',
  backdropFilter: 'blur(8px)',
  WebkitBackdropFilter: 'blur(8px)',
  borderRadius: '9999px',
  font: '500 14px/20px system-ui,sans-serif',
  boxShadow: '0 10px 24px rgba(0,0,0,.25)',
  whiteSpace: 'nowrap',
} as const;

const minimizedPanelStyle = {
  position: 'fixed',
  right: '24px',
  bottom: '24px',
  left: 'auto',
  transform: 'none',
  zIndex: 2147483647,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  width: '44px',
  height: '44px',
  padding: 0,
  background: 'rgba(0,0,0,.5)',
  color: '#fff',
  backdropFilter: 'blur(8px)',
  WebkitBackdropFilter: 'blur(8px)',
  borderRadius: '9999px',
  font: '500 14px/20px system-ui,sans-serif',
  boxShadow: '0 10px 24px rgba(0,0,0,.25)',
  whiteSpace: 'nowrap',
} as const;

const logoStyle = {
  display: 'block',
  width: '18px',
  height: '18px',
} as const;

type TypeUIPanelProps = {
  defaultMinimized?: boolean;
};

export function TypeUIPanel({ defaultMinimized = false }: TypeUIPanelProps) {
  const [minimized, setMinimized] = useState(defaultMinimized);

  return (
    <div aria-label="TypeUI panel" style={minimized ? minimizedPanelStyle : expandedPanelStyle}>
      {minimized ? (
        <button
          type="button"
          aria-label="Maximize TypeUI panel"
          onClick={() => setMinimized(false)}
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            width: '44px',
            height: '44px',
            border: 0,
            padding: 0,
            background: 'transparent',
            color: 'inherit',
            cursor: 'pointer',
          }}
        >
          <Image src="/typeui-logo.svg" alt="TypeUI" width={18} height={18} style={logoStyle} />
        </button>
      ) : (
        <Image src="/typeui-logo.svg" alt="TypeUI" width={18} height={18} style={logoStyle} />
      )}
      <span style={{ display: minimized ? 'none' : 'inline' }}>TypeUI</span>
      <button
        type="button"
        aria-label="Minimize TypeUI panel"
        onClick={() => setMinimized(true)}
        style={{
          display: minimized ? 'none' : 'inline-flex',
          alignItems: 'center',
          justifyContent: 'center',
          width: '24px',
          height: '24px',
          marginLeft: '2px',
          border: '1px solid rgba(255,255,255,.24)',
          borderRadius: '9999px',
          padding: 0,
          background: 'rgba(255,255,255,.1)',
          color: '#fff',
          cursor: 'pointer',
        }}
      >
        <span
          aria-hidden="true"
          style={{
            display: 'block',
            width: '10px',
            height: '2px',
            borderRadius: '9999px',
            background: 'currentColor',
          }}
        />
      </button>
    </div>
  );
}
