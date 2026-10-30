import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

export class DroidRpcError extends Schema.TaggedError<DroidRpcError>()("DroidRpcError", {
  operation: Schema.String,
  code: Schema.optional(Schema.Int),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Droid RPC ${this.operation} failed.`;
  }
}

const droidExecArgs = [
  "exec",
  "--input-format",
  "stream-jsonrpc",
  "--output-format",
  "stream-jsonrpc",
] as const;

const RpcId = Schema.Union([Schema.String, Schema.Number]);
const Record = Schema.Record(Schema.String, Schema.Unknown);
const Envelope = Schema.Struct({
  jsonrpc: Schema.Literal("2.0"),
  type: Schema.Literals(["request", "response", "notification"]),
  id: Schema.optionalKey(RpcId),
  method: Schema.optionalKey(Schema.String),
  params: Schema.optionalKey(Schema.Unknown),
  result: Schema.optionalKey(Schema.Unknown),
  error: Schema.optionalKey(Schema.Struct({ code: Schema.Int, message: Schema.String })),
  factoryProtocolVersion: Schema.optionalKey(Schema.String),
});
const NotificationParams = Schema.Struct({
  sessionId: Schema.optionalKey(Schema.String),
  notification: Record,
});
const decodeEnvelope = Schema.decodeUnknownEffect(Schema.fromJsonString(Envelope));
const decodeNotification = Schema.decodeUnknownEffect(NotificationParams);
const encodeLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export type DroidRpcRequest = {
  readonly id: typeof RpcId.Type;
  readonly method: string;
  readonly params: unknown;
};
export type DroidRpcNotification = typeof NotificationParams.Type;
export type DroidRpcResponse = Pick<typeof Envelope.Type, "result" | "factoryProtocolVersion">;
export type DroidRpcReply =
  | { readonly result: unknown }
  | { readonly error: { readonly code: number; readonly message: string } };

export type DroidRpcConnection = {
  readonly request: (
    method: string,
    params: unknown,
    timeout?: string,
  ) => Effect.Effect<DroidRpcResponse, DroidRpcError>;
  readonly notifications: Stream.Stream<DroidRpcNotification, DroidRpcError>;
  readonly close: Effect.Effect<void>;
};

function defaultReply(method: string): DroidRpcReply {
  if (method === "droid.ask_user") return { result: { cancelled: true, answers: [] } };
  if (method === "droid.request_permission") return { result: { selectedOption: "cancel" } };
  return { error: { code: -32601, message: "Method not supported by T3 Code." } };
}

/**
 * One scope-owned CLI, LF-only JSON-RPC framing, and independent request
 * fibers: waiting for an approval must never block the stdout reader.
 */
export const makeConnection = Effect.fn("DroidRpc.makeConnection")(function* (options: {
  readonly command: string;
  readonly args?: ReadonlyArray<string>;
  readonly cwd?: string;
  readonly env: NodeJS.ProcessEnv;
  readonly closeSession?: boolean;
  readonly onRequest?: (
    request: DroidRpcRequest,
  ) => Effect.Effect<DroidRpcReply | undefined, DroidRpcError>;
}) {
  const scope = yield* Effect.scope;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const command = yield* resolveSpawnCommand(
    options.command,
    [...(options.args ?? droidExecArgs)],
    { env: options.env },
  ).pipe(Effect.mapError((cause) => new DroidRpcError({ operation: "spawn", cause })));
  const child = yield* spawner
    .spawn(
      ChildProcess.make(command.command, command.args, {
        ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
        env: options.env,
        extendEnv: false,
        shell: command.shell,
      }),
    )
    .pipe(Effect.mapError((cause) => new DroidRpcError({ operation: "spawn", cause })));

  const outgoing = yield* Queue.unbounded<Uint8Array, DroidRpcError>();
  const incoming = yield* Queue.unbounded<DroidRpcNotification, DroidRpcError>();
  const down = yield* Deferred.make<never, DroidRpcError>();
  const pending = new Map<string | number, Deferred.Deferred<DroidRpcResponse, DroidRpcError>>();
  const serverRequests = new Map<string | number, DroidRpcRequest>();
  let sequence = 0;
  let closing = false;
  let ended = false;

  const fail = Effect.fnUntraced(function* (error: DroidRpcError) {
    if (!(yield* Deferred.fail(down, error))) return;
    for (const waiter of pending.values()) yield* Deferred.fail(waiter, error);
    pending.clear();
    yield* Queue.fail(outgoing, error);
    yield* Queue.fail(incoming, error);
  });
  const send = Effect.fnUntraced(function* (frame: unknown) {
    const accepted = yield* Queue.offer(
      outgoing,
      new TextEncoder().encode(`${encodeLine(frame)}\n`),
    );
    if (!accepted) return yield* Deferred.await(down);
  });
  const reply = Effect.fnUntraced(function* (request: DroidRpcRequest, response: DroidRpcReply) {
    // Closing and a late user answer can race; the native request gets exactly one reply.
    if (!serverRequests.delete(request.id)) return;
    yield* send({
      type: "response",
      jsonrpc: "2.0",
      factoryApiVersion: "1.0.0",
      id: request.id,
      ...response,
    });
  });
  const request = Effect.fnUntraced(function* (
    method: string,
    params: unknown,
    timeout = "30 seconds",
  ) {
    const id = `t3-${sequence++}`;
    const waiter = yield* Deferred.make<DroidRpcResponse, DroidRpcError>();
    pending.set(id, waiter);
    return yield* send({
      type: "request",
      jsonrpc: "2.0",
      factoryApiVersion: "1.0.0",
      factoryProtocolVersion: "1.201.1",
      id,
      method,
      params,
    }).pipe(
      Effect.andThen(Effect.raceFirst(Deferred.await(waiter), Deferred.await(down))),
      Effect.timeoutOrElse({
        duration: timeout,
        orElse: () => Effect.fail(new DroidRpcError({ operation: method, code: -32000 })),
      }),
      Effect.ensuring(Effect.sync(() => pending.delete(id))),
    );
  });

  const route = Effect.fnUntraced(function* (line: string) {
    const frame = yield* decodeEnvelope(line).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Skipping undecodable native Droid frame", { cause }),
      ),
    );
    if (frame === undefined) return;
    if (frame.type === "response" && frame.id !== undefined) {
      const waiter = pending.get(frame.id);
      if (waiter === undefined) return;
      if (frame.error !== undefined) {
        yield* Deferred.fail(
          waiter,
          new DroidRpcError({
            operation: "request",
            code: frame.error.code,
            cause: frame.error,
          }),
        );
      } else {
        yield* Deferred.succeed(waiter, {
          result: frame.result,
          ...(frame.factoryProtocolVersion === undefined
            ? {}
            : {
                factoryProtocolVersion: frame.factoryProtocolVersion,
              }),
        });
      }
    } else if (frame.type === "request" && frame.id !== undefined) {
      const incomingRequest = { id: frame.id, method: frame.method ?? "", params: frame.params };
      serverRequests.set(frame.id, incomingRequest);
      const handled =
        closing || options.onRequest === undefined
          ? Effect.sync(() => undefined)
          : Effect.suspend(
              () => options.onRequest?.(incomingRequest) ?? Effect.sync(() => undefined),
            );
      yield* handled.pipe(
        // ignoreCause would discard the handler's reply, answering every request with the default.
        Effect.catchCause((cause) =>
          Effect.logWarning("Native Droid request handler failed; sending default reply", {
            cause,
          }),
        ),
        Effect.flatMap((response) =>
          reply(incomingRequest, response ?? defaultReply(incomingRequest.method)),
        ),
        Effect.ignore,
        Effect.forkIn(scope),
      );
    } else if (frame.type === "notification" && frame.method === "droid.session_notification") {
      const notification = yield* decodeNotification(frame.params).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Skipping undecodable native Droid notification", { cause }),
        ),
      );
      if (notification === undefined) return;
      yield* Queue.offer(incoming, notification);
    } else {
      yield* Effect.logWarning("Skipping unknown native Droid RPC frame", {
        type: frame.type,
        method: frame.method,
      });
    }
  });

  yield* child.exitCode.pipe(
    Effect.matchEffect({
      onSuccess: Effect.fn("DroidRpc.onExit")(function* (code) {
        ended = true;
        yield* fail(new DroidRpcError({ operation: "exit", code: Number(code) }));
      }),
      onFailure: Effect.fn("DroidRpc.onExitFailure")(function* (cause) {
        ended = true;
        const error = new DroidRpcError({ operation: "exit", cause });
        yield* fail(error);
      }),
    }),
    Effect.forkIn(scope),
  );
  yield* Effect.gen(function* () {
    let buffer = "";
    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Effect.gen(function* () {
          buffer += chunk;
          let newline;
          while ((newline = buffer.indexOf("\n")) >= 0) {
            const line = buffer.slice(0, newline).replace(/\r$/u, "");
            buffer = buffer.slice(newline + 1);
            if (line.length > 8 * 1024 * 1024)
              return yield* new DroidRpcError({ operation: "frame_size" });
            if (line.length > 0) yield* route(line);
          }
          if (buffer.length > 8 * 1024 * 1024)
            return yield* new DroidRpcError({ operation: "frame_size" });
        }),
      ),
    );
    if (buffer.length > 0) yield* route(buffer);
    yield* fail(new DroidRpcError({ operation: "stdout_closed" }));
  }).pipe(
    Effect.catchCause((cause) => fail(new DroidRpcError({ operation: "read", cause }))),
    Effect.forkIn(scope),
  );
  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(child.stdin),
    Effect.catchCause((cause) => fail(new DroidRpcError({ operation: "write", cause }))),
    Effect.forkIn(scope),
  );
  yield* child.stderr.pipe(Stream.runDrain, Effect.ignore, Effect.forkIn(scope));

  const close = Effect.gen(function* () {
    if (closing) return;
    closing = true;
    for (const incomingRequest of serverRequests.values()) {
      yield* reply(incomingRequest, defaultReply(incomingRequest.method)).pipe(Effect.ignore);
    }
    if (!ended && options.closeSession !== false)
      yield* request("droid.close_session", { reason: "other" }, "2 seconds").pipe(Effect.ignore);
    // Only the captured child handle is signalled; never search for CLI PIDs.
    if (!ended) yield* child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore);
    yield* fail(new DroidRpcError({ operation: "close" }));
  }).pipe(Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()), Effect.uninterruptible);
  yield* Effect.addFinalizer(() => close);
  return {
    request,
    notifications: Stream.fromQueue(incoming),
    close,
  } satisfies DroidRpcConnection;
});
