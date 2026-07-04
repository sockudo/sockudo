/// A global actor that serializes all SockudoClient coordination on a
/// background executor, providing the same data-race safety as @MainActor
/// without blocking the main thread.
@globalActor
public actor SockudoActor {
  public static let shared = SockudoActor()
}
