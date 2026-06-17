import URLLocation from "./url_location";

interface SocketHooks {
  getReceiveURL(url: URLLocation, session: string): string;
  onHeartbeat(socket): void;
  sendHeartbeat(socket): void;
  onFinished(socket, status): void;
}

export default SocketHooks;
