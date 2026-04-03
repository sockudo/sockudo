export default defineAppConfig({
  docus: {
    locale: "en",
  },
  seo: {
    title: "Sockudo Docs — Realtime Infrastructure for Pusher-Compatible Apps",
    titleTemplate: "%s · Sockudo Docs",
    description:
      "Build and run Sockudo, the open-source realtime server and client stack for Pusher-compatible apps.",
  },
  header: {
    logo: {
      light: "/sockudo-logo/sockudo-logo-black.svg",
      dark: "/sockudo-logo/sockudo-logo-white.svg",
      alt: "Sockudo",
    },
  },
  socials: {
    github: "https://github.com/sockudo/sockudo",
    npm: "https://www.npmjs.com/package/@sockudo/client",
    x: "https://x.com/sockudorealtime",
  },
  toc: {
    title: "On This Page",
    bottom: {
      title: "Sockudo",
      links: [
        {
          icon: "i-simple-icons-github",
          label: "sockudo",
          to: "https://github.com/sockudo/sockudo",
          target: "_blank",
        },
        {
          icon: "i-simple-icons-github",
          label: "sockudo-js",
          to: "https://github.com/sockudo/sockudo-js",
          target: "_blank",
        },
        {
          icon: "i-simple-icons-npm",
          label: "@sockudo/client",
          to: "https://www.npmjs.com/package/@sockudo/client",
          target: "_blank",
        },
      ],
    },
  },
  github: false,
  ui: {
    colors: {
      primary: "violet",
      neutral: "slate",
    },
    contentNavigation: {
      slots: {
        linkLeadingIcon: "size-4 mr-1",
      },
      defaultVariants: {
        variant: "link",
      },
    },
    pageLinks: {
      slots: {
        linkLeadingIcon: "size-4",
      },
    },
    pageFeature: {
      slots: {
        root: "border border-default rounded-xl p-6 md:p-7 transition-all duration-300 hover:border-[#7938d3]/40 hover:shadow-lg hover:shadow-[#7938d3]/10 hover:-translate-y-1",
        wrapper: "flex flex-col items-center text-center h-full",
        leading: "mb-3 p-0",
        leadingIcon: "size-5 md:size-6 text-[#7938d3]",
        title: "text-base font-semibold text-highlighted",
        description:
          "mt-2 text-sm md:text-base text-muted leading-relaxed max-w-prose",
      },
      defaultVariants: {
        orientation: "vertical",
      },
    },
  },
});
