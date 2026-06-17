/**
 * Protocol version prefix helpers.
 *
 * Protocol v1 uses the original Pusher event prefix ("pusher:" / "pusher_internal:").
 * Protocol v2 (Sockudo-native) uses "sockudo:" / "sockudo_internal:".
 *
 * The active prefixes are set once at SDK init time via {@link setProtocolVersion}
 * and read everywhere else through the exported constants.
 */

let _version: number = 7;
let _eventPrefix: string = "pusher:";
let _internalPrefix: string = "pusher_internal:";

export function setProtocolVersion(version: number): void {
  _version = version;
  if (version === 2) {
    _eventPrefix = "sockudo:";
    _internalPrefix = "sockudo_internal:";
  } else {
    _eventPrefix = "pusher:";
    _internalPrefix = "pusher_internal:";
  }
}

export function protocolVersion(): number {
  return _version;
}

export function eventPrefix(): string {
  return _eventPrefix;
}

export function internalPrefix(): string {
  return _internalPrefix;
}

export function prefixedEvent(name: string): string {
  return _eventPrefix + name;
}

export function prefixedInternal(name: string): string {
  return _internalPrefix + name;
}

export function isInternalEvent(name: string): boolean {
  return name.indexOf(_internalPrefix) === 0;
}

export function isPlatformEvent(name: string): boolean {
  return name.indexOf(_eventPrefix) === 0;
}
