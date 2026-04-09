<script setup lang="ts">
defineProps({
  title: {
    type: String,
    default: 'Sockudo Docs',
  },
  description: {
    type: String,
    default:
      'Open-source realtime infrastructure for Pusher-compatible apps.',
  },
})
</script>

<template>
  <div
    :style="{
      width: '1200px',
      height: '630px',
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'stretch',
      justifyContent: 'space-between',
      background:
        'radial-gradient(circle at top left, #4c1d95 0%, rgba(76,29,149,0.25) 28%, transparent 48%), radial-gradient(circle at bottom right, #312e81 0%, rgba(49,46,129,0.28) 24%, transparent 44%), linear-gradient(135deg, #09090f 0%, #111126 52%, #18183a 100%)',
      fontFamily: 'sans-serif',
      position: 'relative',
      overflow: 'hidden',
      padding: '48px 56px',
    }"
  >
    <div
      :style="{
        position: 'absolute',
        top: '0',
        left: '0',
        right: '0',
        height: '4px',
        backgroundColor: '#7938d3',
      }"
    />

    <div
      :style="{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
      }"
    >
      <div
        :style="{
          display: 'flex',
          alignItems: 'center',
          gap: '16px',
        }"
      >
        <div
          :style="{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            width: '72px',
            height: '72px',
            borderRadius: '22px',
            background:
              'linear-gradient(180deg, rgba(121,56,211,0.28) 0%, rgba(121,56,211,0.12) 100%)',
            border: '1px solid rgba(196,181,253,0.2)',
          }"
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 420 366"
            width="44"
            height="44"
          >
            <path
              fill="#2a2a3e"
              d="m417.1,182.95c0,101.05-75.63,182.96-168.92,182.96s-168.91-81.91-168.91-182.96c0-12.16,1.1-24.04,3.2-35.54,3.7-20.44,10.56-39.65,20.01-57.03C131.78,36.3,186.06,0,248.17,0s116.39,36.3,145.72,90.38c9.45,17.38,16.31,36.59,20.01,57.03,2.1,11.49,3.2,23.37,3.2,35.54Z"
            />
            <path
              fill="#7938d3"
              d="m413.9,147.41c-3.7-20.44-10.56-39.65-20.01-57.03H102.51C81.83,59.35,46.52,38.9,6.42,38.9c-1.96,0-3.91.06-5.86.15,8.49,32.92,31.13,60.13,61.05,74.78C35.46,118.16,13.15,133.91,0,155.78c10.27,4.16,21.5,6.44,33.25,6.44,18.21,0,35.13-5.48,49.22-14.87,0,.02,0,.04-.01.06h331.43Z"
            />
            <path
              fill="#fefcfb"
              d="m324.17,235.47h-151.98c-24.85,0-45-20.15-45-45v-41.93c0-3.97,3.32-7.13,7.29-6.91,13.07.71,40.16,2.13,51.04,2.38,31.32.75,93.99.75,125.31,0,10.88-.26,37.97-1.68,51.04-2.38,3.96-.21,7.29,2.94,7.29,6.91v41.93c0,24.85-20.15,45-45,45Z"
            />
            <path
              fill="#1a1a2e"
              d="m216.5,200.84c-6.56,6.78-16.09,10.66-26.49,9.57-15.53-1.63-27.78-14.64-28.55-30.24-.36-7.22,1.66-13.94,5.38-19.45.72-1.08,2.28-1.52,3.46-1l41.87,18.74c8.8,3.94,11.04,15.44,4.34,22.37Z"
            />
            <path
              fill="#1a1a2e"
              d="m279.86,200.84c6.56,6.78,16.09,10.66,26.49,9.57,15.53-1.63,27.78-14.64,28.55-30.24.36-7.22-1.66-13.94-5.38-19.45-.72-1.08-2.28-1.52-3.46-1l-41.87,18.74c-8.8,3.94-11.04,15.44-4.34,22.37Z"
            />
          </svg>
        </div>

        <div
          :style="{
            display: 'flex',
            flexDirection: 'column',
          }"
        >
          <div
            :style="{
              fontSize: '20px',
              fontWeight: '700',
              color: '#ffffff',
              letterSpacing: '0.08em',
            }"
          >
            SOCKUDO
          </div>
          <div
            :style="{
              fontSize: '14px',
              color: '#c4b5fd',
              textTransform: 'uppercase',
              letterSpacing: '0.14em',
            }"
          >
            Realtime docs
          </div>
        </div>
      </div>

      <div
        :style="{
          display: 'flex',
          alignItems: 'center',
          padding: '10px 16px',
          borderRadius: '999px',
          border: '1px solid rgba(196,181,253,0.2)',
          backgroundColor: 'rgba(17,17,38,0.72)',
          fontSize: '16px',
          color: '#ddd6fe',
        }"
      >
        sockudo.io
      </div>
    </div>

    <div
      :style="{
        display: 'flex',
        flexDirection: 'column',
        gap: '22px',
        maxWidth: '860px',
      }"
    >
      <div
        :style="{
          display: 'flex',
          alignItems: 'center',
          alignSelf: 'flex-start',
          padding: '8px 14px',
          borderRadius: '999px',
          backgroundColor: 'rgba(121,56,211,0.16)',
          border: '1px solid rgba(121,56,211,0.28)',
          color: '#c4b5fd',
          fontSize: '16px',
          fontWeight: '600',
          letterSpacing: '0.06em',
          textTransform: 'uppercase',
        }"
      >
        Open-source Pusher-compatible stack
      </div>

      <div
        :style="{
          fontSize: '68px',
          fontWeight: '800',
          color: '#ffffff',
          lineHeight: '1.05',
          letterSpacing: '-0.04em',
          maxWidth: '860px',
        }"
      >
        {{ title }}
      </div>

      <div
        :style="{
          fontSize: '28px',
          fontWeight: '400',
          color: '#b7b9d3',
          lineHeight: '1.4',
          maxWidth: '820px',
        }"
      >
        {{ description }}
      </div>
    </div>

    <div
      :style="{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: '20px',
      }"
    >
      <div
        :style="{
          display: 'flex',
          gap: '12px',
        }"
      >
        <div
          :style="{
            padding: '12px 18px',
            borderRadius: '16px',
            border: '1px solid rgba(148,163,184,0.16)',
            backgroundColor: 'rgba(15,23,42,0.6)',
            color: '#e2e8f0',
            fontSize: '18px',
          }"
        >
          Rust server
        </div>
        <div
          :style="{
            padding: '12px 18px',
            borderRadius: '16px',
            border: '1px solid rgba(148,163,184,0.16)',
            backgroundColor: 'rgba(15,23,42,0.6)',
            color: '#e2e8f0',
            fontSize: '18px',
          }"
        >
          Official clients
        </div>
        <div
          :style="{
            padding: '12px 18px',
            borderRadius: '16px',
            border: '1px solid rgba(148,163,184,0.16)',
            backgroundColor: 'rgba(15,23,42,0.6)',
            color: '#e2e8f0',
            fontSize: '18px',
          }"
        >
          Scaling guides
        </div>
      </div>
      <div
        :style="{
          fontSize: '18px',
          color: '#8b8fb2',
        }"
      >
        Setup, scaling, API, security, and integrations
      </div>
    </div>
  </div>
</template>
