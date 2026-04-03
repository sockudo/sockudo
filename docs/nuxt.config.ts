import { defineOrganization } from "nuxt-schema-org/schema";

export default defineNuxtConfig({
  modules: ["@nuxtjs/sitemap", "nuxt-schema-org", "nuxt-link-checker"],

  css: ["~/assets/css/brand.css"],

  image: {
    provider: "none",
  },

  site: {
    url: "https://sockudo.io",
    name: "Sockudo Docs",
    description:
      "Open-source realtime infrastructure for Pusher-compatible apps, with production docs for Sockudo server and clients.",
    defaultLocale: "en",
  },

  robots: {
    credits: false,
  },

  sitemap: {
    sortEntries: true,
  },

  schemaOrg: {
    identity: defineOrganization({
      name: "Sockudo",
      url: "https://sockudo.io",
      logo: "/logo.svg",
      sameAs: [
        "https://github.com/sockudo/sockudo",
        "https://www.npmjs.com/package/@sockudo/client",
        "https://x.com/sockudorealtime",
      ],
    }),
  },

  linkChecker: {
    report: {
      html: true,
    },
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
