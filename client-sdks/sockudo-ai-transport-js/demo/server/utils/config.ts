export interface DemoConfig {
  appBaseUrl: string;
  appId: string;
  appKey: string;
  appSecret: string;
  channelName: string;
  host: string;
  model: string;
  port: number;
}

export function demoConfig(): DemoConfig {
  const port = Number(process.env.SOCKUDO_PORT ?? "6001");
  return {
    appBaseUrl:
      process.env.SOCKUDO_DEMO_APP_BASE_URL ?? `http://127.0.0.1:${process.env.PORT ?? "5174"}`,
    appId: process.env.SOCKUDO_APP_ID ?? "demo-app",
    appKey: process.env.SOCKUDO_APP_KEY ?? "demo-key",
    appSecret: process.env.SOCKUDO_APP_SECRET ?? "demo-secret",
    channelName: process.env.SOCKUDO_CHANNEL_NAME ?? "private-ai-nuxt",
    host: process.env.SOCKUDO_HOST ?? "127.0.0.1",
    model: process.env.SOCKUDO_DEMO_MODEL ?? "openai/gpt-5-mini",
    port,
  };
}

export function publicDemoConfig() {
  const config = demoConfig();
  return {
    appKey: config.appKey,
    channelName: config.channelName,
    host: config.host,
    model: config.model,
    port: config.port,
    usingGatewayKey: Boolean(process.env.AI_GATEWAY_API_KEY ?? process.env.VERCEL_API_KEY),
  };
}
