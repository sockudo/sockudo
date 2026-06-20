/**
 * Lifecycle phase configuration.
 */
export interface PhaseConfig<TContext, TEvent> {
  /** Stable phase name. */
  name: string;
  /** Builds the synthetic event for this phase. */
  buildEvent(scopeId: string, context: TContext): TEvent;
}

/**
 * Per-scope phase synthesis helper.
 */
export interface LifecycleTracker<TContext, TEvent> {
  /** Emits missing phase events in configured order. */
  ensurePhases(scopeId: string, context: TContext): TEvent[];
  /** Marks a phase as already emitted. */
  markEmitted(scopeId: string, phase: string): void;
  /** Resets a phase and every phase after it for a scope. */
  resetPhase(scopeId: string, phase: string): void;
  /** Clears all phase state for a scope. */
  clearScope(scopeId: string): void;
}

/**
 * Creates a per-scope lifecycle tracker.
 */
export function createLifecycleTracker<TContext, TEvent>(
  phases: readonly PhaseConfig<TContext, TEvent>[],
): LifecycleTracker<TContext, TEvent> {
  const emittedByScope = new Map<string, Set<string>>();
  const phaseNames = phases.map((phase) => phase.name);
  return {
    ensurePhases(scopeId, context) {
      const emitted = ensureScope(emittedByScope, scopeId);
      const events: TEvent[] = [];
      for (const phase of phases) {
        if (!emitted.has(phase.name)) {
          events.push(phase.buildEvent(scopeId, context));
          emitted.add(phase.name);
        }
      }
      return events;
    },
    markEmitted(scopeId, phase) {
      ensureScope(emittedByScope, scopeId).add(phase);
    },
    resetPhase(scopeId, phase) {
      const emitted = ensureScope(emittedByScope, scopeId);
      const start = phaseNames.indexOf(phase);
      if (start === -1) {
        return;
      }
      for (let index = start; index < phaseNames.length; index += 1) {
        const name = phaseNames[index];
        if (name !== undefined) {
          emitted.delete(name);
        }
      }
    },
    clearScope(scopeId) {
      emittedByScope.delete(scopeId);
    },
  };
}

function ensureScope(emittedByScope: Map<string, Set<string>>, scopeId: string): Set<string> {
  let emitted = emittedByScope.get(scopeId);
  if (!emitted) {
    emitted = new Set();
    emittedByScope.set(scopeId, emitted);
  }
  return emitted;
}
