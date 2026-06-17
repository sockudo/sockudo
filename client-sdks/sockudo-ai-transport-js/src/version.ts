declare const __SOCKUDO_AI_TRANSPORT_VERSION__: string | undefined;

/**
 * Current `@sockudo/ai-transport` package version injected by the build.
 *
 * This value is read-only and does not perform I/O.
 *
 * @defaultValue `"0.0.0-dev"` when no build-time version is injected.
 */
export const version: string =
  typeof __SOCKUDO_AI_TRANSPORT_VERSION__ === "string"
    ? __SOCKUDO_AI_TRANSPORT_VERSION__
    : "0.0.0-dev";
