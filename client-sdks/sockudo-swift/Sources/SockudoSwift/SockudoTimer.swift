import Foundation

/// A cancellable timer that runs on @SockudoActor using cooperative Task
/// scheduling. Replaces Timer.scheduledTimer / RunLoop.main which require
/// a run loop that does not exist on the actor's background executor.
@SockudoActor
final class SockudoTimer {
  private var task: Task<Void, Never>?

  /// Whether a timer is currently pending (true after schedule, false after cancel or one-shot fires).
  var isActive: Bool { task != nil }

  /// Schedule a one-shot action after `seconds`. Cancels any pending timer.
  func schedule(after seconds: TimeInterval, _ action: @escaping @SockudoActor () -> Void) {
    task?.cancel()
    task = Task { [weak self] in
      try? await Task.sleep(nanoseconds: UInt64(max(0, seconds) * 1_000_000_000))
      guard let self, !Task.isCancelled else { return }
      // Clear BEFORE running the action: past the guard we are provably the
      // current task (a replacement would have cancelled us), and the action
      // may reschedule this same timer — clearing after would orphan that new
      // task, leaving it uncancellable (caused spurious pong-timeout reconnects).
      self.task = nil
      action()
    }
  }

  /// Schedule a repeating action every `seconds`. Cancels any pending timer.
  /// Sleeps FIRST then fires (matches Timer.scheduledTimer(repeats: true) behavior).
  func scheduleRepeating(every seconds: TimeInterval, _ action: @escaping @SockudoActor () -> Void) {
    task?.cancel()
    task = Task {
      while !Task.isCancelled {
        try? await Task.sleep(nanoseconds: UInt64(max(0, seconds) * 1_000_000_000))
        guard !Task.isCancelled else { return }
        action()
      }
    }
  }

  /// Cancel any pending timer. isActive becomes false.
  func cancel() {
    task?.cancel()
    task = nil
  }
}
