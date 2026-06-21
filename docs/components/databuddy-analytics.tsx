'use client';

import { Databuddy } from '@databuddy/sdk/react';

export function DatabuddyAnalytics() {
  return (
    <Databuddy
      clientId="5aFTKbNqr8XkSznh3u3F3"
      trackHashChanges={true}
      trackAttributes={true}
      trackOutgoingLinks={true}
      trackInteractions={true}
      trackWebVitals={true}
      trackErrors={true}
    />
  );
}
