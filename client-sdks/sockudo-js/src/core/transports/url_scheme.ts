export interface URLSchemeParams {
  useTLS: boolean;
  hostTLS: string;
  hostNonTLS: string;
  httpPath: string;
  echoMessages?: boolean;
  wireFormat?: "json" | "messagepack" | "msgpack" | "protobuf" | "proto";
}

interface URLScheme {
  getInitial(key: string, params: any): string;
  getPath?(key: string, options: any): string;
}

export default URLScheme;
