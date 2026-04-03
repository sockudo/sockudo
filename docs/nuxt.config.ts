export default defineNuxtConfig({
  css: ["~/assets/css/brand.css"],

  image: {
    provider: "none",
  },

  site: {
    url: "https://sockudo.io",
  },

  ogImage: {
    defaults: {
      component: "Sockudo",
      props: {
        title: "Sockudo Docs",
        description:
          "Open-source realtime infrastructure for Pusher-compatible apps.",
      },
    },
  },

  app: {
    head: {
      link: [{ rel: "icon", type: "image/svg+xml", href: "/favicon.svg" }],
      meta: [{ name: "theme-color", content: "#7938d3" }],
      script: [
        {
          src: "https://cdn.databuddy.cc/databuddy.js",
          "data-client-id": "5aFTKbNqr8XkSznh3u3F3",
          "data-track-hash-changes": "true",
          "data-track-attributes": "true",
          "data-track-outgoing-links": "true",
          "data-track-interactions": "true",
          "data-track-scroll-depth": "true",
          "data-track-web-vitals": "true",
          "data-track-errors": "true",
          crossorigin: "anonymous",
          async: true,
        },
      ],
    },
  },
});
