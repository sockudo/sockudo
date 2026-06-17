let _wireFormat: "json" | "messagepack" | "protobuf" = "json";

export function setWireFormat(
  format: "json" | "messagepack" | "msgpack" | "protobuf" | "proto" | undefined,
): void {
  switch (format) {
    case "messagepack":
    case "msgpack":
      _wireFormat = "messagepack";
      break;
    case "protobuf":
    case "proto":
      _wireFormat = "protobuf";
      break;
    default:
      _wireFormat = "json";
      break;
  }
}

export function wireFormat(): "json" | "messagepack" | "protobuf" {
  return _wireFormat;
}
