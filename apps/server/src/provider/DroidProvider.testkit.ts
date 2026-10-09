import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

const decodeRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ id: Schema.String, method: Schema.String })),
);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
export const makeDroidDiscoverySpawner = (options?: {
  readonly authStatus?: string | null;
  readonly protocol?: string;
  readonly models?: ReadonlyArray<unknown>;
}) =>
  Effect.sync(() => {
    const launches: ChildProcess.StandardCommand[] = [];
    const methods: string[] = [];
    const encoder = new TextEncoder();
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.gen(function* () {
        if (!ChildProcess.isStandardCommand(command))
          return yield* Effect.die("Unexpected pipeline");
        launches.push(command);
        const version = command.args.includes("--version");
        const doctor = command.args.includes("doctor");
        const stdout = yield* Queue.unbounded<Uint8Array>();
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(999_999_999),
          exitCode:
            version || doctor ? Effect.succeed(ChildProcessSpawner.ExitCode(0)) : Effect.never,
          isRunning: Effect.succeed(!version && !doctor),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.forEach((chunk: Uint8Array) =>
            Effect.gen(function* () {
              const request = decodeRequest(new TextDecoder().decode(chunk).trim());
              methods.push(request.method);
              const result =
                request.method === "droid.list_models"
                  ? {
                      models: options?.models ?? [
                        {
                          id: "custom:GPT-6 Luna-12",
                          displayName: "GPT-6 Luna",
                          supportedReasoningEfforts: ["none", "low", "medium", "high"],
                          defaultReasoningEffort: "medium",
                          isCustom: true,
                        },
                        {
                          id: "custom:Claude Sonnet 5.5-15",
                          displayName: "Claude: Sonnet 5.5",
                          supportedReasoningEfforts: ["off", "high", "max"],
                          defaultReasoningEffort: "high",
                          isCustom: true,
                        },
                      ],
                    }
                  : request.method === "droid.list_commands"
                    ? {
                        commands: [
                          {
                            name: "review",
                            description: "Review changes",
                            argumentHint: "<files>",
                          },
                        ],
                      }
                    : {
                        skills: [
                          {
                            name: "verify",
                            description: "Verify work",
                            location: command.options.cwd === "/project" ? "project" : "personal",
                            filePath: "/skills/verify/SKILL.md",
                            enabled: true,
                            userInvocable: true,
                            content: "Never send this content",
                          },
                        ],
                      };
              yield* Queue.offer(
                stdout,
                encoder.encode(
                  `${encodeJson({
                    jsonrpc: "2.0",
                    type: "response",
                    factoryApiVersion: "1.0.0",
                    factoryProtocolVersion: options?.protocol ?? "1.246.0",
                    id: request.id,
                    result,
                  })}\n`,
                ),
              );
            }),
          ),
          stdout: version
            ? Stream.succeed(encoder.encode("0.234.0\n"))
            : doctor
              ? Stream.succeed(
                  encoder.encode(
                    encodeJson({
                      results:
                        options?.authStatus === null
                          ? []
                          : [{ id: "auth.verify", status: options?.authStatus ?? "pass" }],
                    }),
                  ),
                )
              : Stream.fromQueue(stdout),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        });
      }),
    );
    return { spawner, launches, methods };
  });
