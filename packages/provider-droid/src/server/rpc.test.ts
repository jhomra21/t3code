import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

import * as DroidRpc from "./rpc.ts";

const peer = `
let buffer = "";
const send = (frame) => {
  const text = JSON.stringify({jsonrpc:"2.0", factoryApiVersion:"1.0.0",
    factoryProtocolVersion:"1.246.0", ...frame}) + "\\n";
  process.stdout.write(text.slice(0, 7));
  process.stdout.write(text.slice(7));
};
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let index;
  while ((index = buffer.indexOf("\\n")) >= 0) {
    const frame = JSON.parse(buffer.slice(0, index)); buffer = buffer.slice(index + 1);
    if (frame.type === "response") {
      send({type:"notification",method:"droid.session_notification",
        params:{notification:{type:"reply_seen",reply:frame}}});
    } else if (frame.method === "malformed") {
      process.stdout.write("not-json\\n");
      send({type:"notification",method:"droid.session_notification",params:{notification:12}});
      send({type:"future-frame",method:"new"});
      send({type:"response",id:frame.id,result:{ok:true}});
    } else if (frame.method === "exit") process.exit(9);
    else if (frame.method === "requests") {
      for (const method of ["droid.ask_user","droid.request_permission","unknown"]) {
        send({type:"request",id:method,method,params:{}});
      }
      send({type:"response",id:frame.id,result:{}});
    } else {
      send({type:"response",id:frame.id,result:{sessionId:"session",text:"A\\u2028B"}});
    }
  }
});
`;

describe("DroidRpc", () => {
  it.effect("skips malformed and unknown frames while still correlating the next response", () =>
    Effect.gen(function* () {
      const rpc = yield* DroidRpc.makeConnection({
        command: process.execPath,
        args: ["-e", peer],
        env: process.env,
      });
      assert.deepEqual((yield* rpc.request("malformed", {})).result, { ok: true });
      assert.deepEqual((yield* rpc.request("after-malformed", {})).result, {
        sessionId: "session",
        text: "A\u2028B",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect("correlates fragmented frames without splitting Unicode separators", () =>
    Effect.gen(function* () {
      const rpc = yield* DroidRpc.makeConnection({
        command: process.execPath,
        args: ["-e", peer],
        env: process.env,
      });
      const replies = yield* Effect.all([rpc.request("first", {}), rpc.request("second", {})], {
        concurrency: 2,
      });
      assert.deepEqual(
        replies.map((frame) => frame.result),
        [
          { sessionId: "session", text: "A\u2028B" },
          { sessionId: "session", text: "A\u2028B" },
        ],
      );
      assert.equal(replies[0]?.factoryProtocolVersion, "1.246.0");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("answers missing and failed handlers, and rejects unknown requests", () =>
    Effect.gen(function* () {
      const rpc = yield* DroidRpc.makeConnection({
        command: process.execPath,
        args: ["-e", peer],
        env: process.env,
        onRequest: (request) =>
          request.method === "droid.ask_user"
            ? Effect.die("handler failed")
            : Effect.sync(() => undefined),
      });
      yield* rpc.request("requests", {});
      const replies = yield* rpc.notifications.pipe(Stream.take(3), Stream.runCollect);
      assert.deepEqual(
        replies.map((entry) => entry.notification["reply"]),
        [
          {
            type: "response",
            jsonrpc: "2.0",
            factoryApiVersion: "1.0.0",
            id: "droid.ask_user",
            result: { cancelled: true, answers: [] },
          },
          {
            type: "response",
            jsonrpc: "2.0",
            factoryApiVersion: "1.0.0",
            id: "droid.request_permission",
            result: { selectedOption: "cancel" },
          },
          {
            type: "response",
            jsonrpc: "2.0",
            factoryApiVersion: "1.0.0",
            id: "unknown",
            error: { code: -32601, message: "Method not supported by T3 Code." },
          },
        ],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("fails pending requests when the captured process exits", () =>
    Effect.gen(function* () {
      const rpc = yield* DroidRpc.makeConnection({
        command: process.execPath,
        args: ["-e", peer],
        env: process.env,
      });
      const result = yield* rpc.request("exit", {}).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
