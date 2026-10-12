const MINIMUM_PROTOCOL = { major: 1, minor: 246, patch: 0 };

const parseDroidProtocolVersion = (version: string) => {
  const match = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.exec(version);
  if (match === null) return null;
  const major = Number(match[1]);
  const minor = Number(match[2]);
  const patch = Number(match[3]);
  return Number.isSafeInteger(major) && Number.isSafeInteger(minor) && Number.isSafeInteger(patch)
    ? { major, minor, patch }
    : null;
};

export function isAcceptedDroidProtocolVersion(version: string) {
  const parsed = parseDroidProtocolVersion(version);
  return (
    parsed !== null &&
    parsed.major === MINIMUM_PROTOCOL.major &&
    (parsed.minor > MINIMUM_PROTOCOL.minor ||
      (parsed.minor === MINIMUM_PROTOCOL.minor && parsed.patch >= MINIMUM_PROTOCOL.patch))
  );
}
