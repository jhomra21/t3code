import { assert, describe, it } from "@effect/vitest";

import { isAcceptedDroidProtocolVersion } from "./protocolVersion.ts";

describe("Droid protocol version compatibility", () => {
  it("accepts the supported 1.x range and rejects malformed, incompatible, or older versions", () => {
    assert.isTrue(isAcceptedDroidProtocolVersion("1.246.0"));
    assert.isTrue(isAcceptedDroidProtocolVersion("1.253.0"));
    assert.isFalse(isAcceptedDroidProtocolVersion("1.245.99"));
    assert.isFalse(isAcceptedDroidProtocolVersion("2.246.0"));
    assert.isFalse(isAcceptedDroidProtocolVersion("invalid"));
  });
});
