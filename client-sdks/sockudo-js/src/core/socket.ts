interface Socket {
  send(payload: unknown): void;
  ping?(): void;
  close(code?: unknown, reason?: unknown);
  sendRaw?(payload: unknown): boolean;

  onopen?(evt?: unknown): void;
  onerror?(error: unknown): void;
  onclose?(closeEvent: unknown): void;
  onmessage?(message: unknown): void;
  onactivity?(): void;
}

export default Socket;
