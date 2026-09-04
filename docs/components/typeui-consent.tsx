'use client';

import { useEffect, useState, useSyncExternalStore } from 'react';

declare global {
  interface Window {
    enableTypeUIInsights?: () => void;
    disableTypeUIInsights?: () => void;
    trackTypeUIEvent?: (name: string, options?: { value?: number; currency?: string }) => boolean;
  }
}

type Consent = 'granted' | 'denied' | null;
const storageKey = 'sockudo.typeui.analytics-consent';
const consentEvent = 'sockudo:typeui-consent';
// Retain the current choice even when browser storage is unavailable.
let sessionConsent: Consent | undefined;

function getConsent(): Consent {
  if (sessionConsent !== undefined) return sessionConsent;
  try {
    const stored = window.localStorage.getItem(storageKey);
    return stored === 'granted' || stored === 'denied' ? stored : null;
  } catch {
    return null;
  }
}

function applyConsent(consent: Consent) {
  if (consent === 'granted') {
    window.enableTypeUIInsights?.();
  } else {
    window.disableTypeUIInsights?.();
  }
}

function subscribe(onChange: () => void) {
  function onStorage(event: StorageEvent) {
    if (event.key !== storageKey && event.key !== null) return;
    sessionConsent = undefined;
    applyConsent(getConsent());
    onChange();
  }

  window.addEventListener('storage', onStorage);
  window.addEventListener(consentEvent, onChange);
  return () => {
    window.removeEventListener('storage', onStorage);
    window.removeEventListener(consentEvent, onChange);
  };
}

function getServerConsent(): Consent {
  return null;
}

export function TypeUIConsent() {
  const consent = useSyncExternalStore(subscribe, getConsent, getServerConsent);
  const [preferencesOpen, setPreferencesOpen] = useState(false);

  useEffect(() => {
    applyConsent(consent);
  }, [consent]);

  function chooseConsent(nextConsent: Exclude<Consent, null>) {
    sessionConsent = nextConsent;
    // Revoke immediately, including while the tracker script is still loading.
    applyConsent(nextConsent);
    try {
      window.localStorage.setItem(storageKey, nextConsent);
    } catch {
      // The in-memory choice applies until this page is reloaded.
    }
    window.dispatchEvent(new Event(consentEvent));
    setPreferencesOpen(false);
  }

  const showPreferences = consent === null || preferencesOpen;
  const buttonClass = 'min-h-11 rounded-lg border border-fd-border px-3 py-2 text-sm font-medium hover:bg-fd-accent focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-fd-ring';

  return (
    <aside
      aria-label="TypeUI analytics consent"
      data-typeui-ignore
      className="fixed bottom-4 left-4 z-50 max-w-[calc(100vw-2rem)] rounded-xl border border-fd-border bg-fd-background p-3 text-fd-foreground shadow-lg"
    >
      {showPreferences ? (
        <div id="typeui-analytics-preferences" className="w-80 max-w-full">
          <h2 className="text-sm font-semibold">TypeUI analytics</h2>
          <p className="mt-2 text-sm text-fd-muted-foreground">
            Allow TypeUI Insights to measure page visits and clicks? You can change this choice at any time.
          </p>
          <div className="mt-3 flex flex-wrap gap-2">
            <button type="button" className={buttonClass} onClick={() => chooseConsent('granted')}>
              Allow analytics
            </button>
            <button type="button" className={buttonClass} onClick={() => chooseConsent('denied')}>
              {consent === 'granted' ? 'Withdraw consent' : 'Decline analytics'}
            </button>
            {consent !== null && (
              <button type="button" className={buttonClass} onClick={() => setPreferencesOpen(false)}>
                Close
              </button>
            )}
          </div>
        </div>
      ) : (
        <button
          type="button"
          className={buttonClass}
          aria-expanded={false}
          aria-controls="typeui-analytics-preferences"
          onClick={() => setPreferencesOpen(true)}
        >
          Analytics preferences
        </button>
      )}
    </aside>
  );
}
