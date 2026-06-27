import type { AppendMode } from "../options";

export interface URLSchemeParams {
  useTLS: boolean;
  hostTLS: string;
  hostNonTLS: string;
  httpPath: string;
  echoMessages?: boolean;
  wireFormat?: "json" | "messagepack" | "msgpack" | "protobuf" | "proto";
  appendMode?: AppendMode;
}

interface URLScheme {
  getInitial(key: string, params: any): string;
  getPath?(key: string, options: any): string;
}

export default URLScheme;
