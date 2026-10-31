/**
 * Records a Droid JSON-RPC transcript through the real orchestrator, adapter,
 * and RPC connection. Only the process spawner is wrapped to tee stdio.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import type { ProviderReplayEntry, ProviderReplayTranscript } from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Duration from "effect/Duration";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { TestClock } from "effect/testing";

import {
  DROID_REPLAY_ANY,
  DROID_STREAM_JSONRPC_PROTOCOL,
  DroidOrchestratorReplayHarness,
  layer as droidReplayLayer,
  makeDroidRecordingSpawner,
} from "../src/orchestration-v2/Adapters/DroidAdapterV2.testkit.ts";
import {
  DROID_PROVIDER,
  makeDroidEnvironment,
  parseDroidVersion,
} from "@t3tools/provider-droid/testing";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { provideDeterministicTestRuntime } from "../src/orchestration-v2/testkit/DeterministicRuntime.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "../src/orchestration-v2/testkit/fixtures/index.ts";
import {
  DROID_MODEL_SELECTION,
  materializeFixtureInput,
} from "../src/orchestration-v2/testkit/fixtures/shared.ts";
import { runOrchestratorV2ProviderReplayScenario } from "../src/orchestration-v2/testkit/ProviderReplayHarness.ts";
import {
  checkpointWorkspace,
  makeCheckpointWorkspace,
} from "@t3tools/provider-testing/replayWorkspace";
import { materializeReplayTranscriptWorkspace } from "@t3tools/provider-testing/replayTranscript";

function readArgValue(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

const scenario = readArgValue("--scenario");
const fixture = ORCHESTRATOR_REPLAY_FIXTURES.find((entry) => entry.name === scenario);
const variant = fixture?.providers.find((provider) => provider.driver === DROID_PROVIDER);
if (fixture === undefined || variant === undefined) {
  throw new Error("Pass --scenario with a fixture that registers Factory Droid.");
}

const CLOCK_TICK = Duration.millis(20);
const droidBinary = process.env.T3_DROID_BIN ?? "droid";
const home = process.env.HOME ?? "";
const UUID_PATTERN = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/giu;
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeEntries(
  entries: ReadonlyArray<ProviderReplayEntry>,
  workspaces: ReadonlyArray<string>,
): ReadonlyArray<ProviderReplayEntry> {
  const uuids = new Map<string, string>();
  // T3 allocates these ids deterministically, and replay compares them to the recorded request
  // frames, so any id T3 minted must stay as-is. An id Droid minted first and T3 later sent back,
  // such as the session id on `droid.load_session`, is Droid's and gets rewritten.
  const seen = new Set<string>();
  const sentByT3 = new Set<string>();
  for (const entry of entries) {
    if (entry.type === "runtime_exit") continue;
    for (const match of JSON.stringify(entry.frame).match(UUID_PATTERN) ?? []) {
      const uuid = match.toLowerCase();
      if (entry.type === "expect_outbound" && !seen.has(uuid)) sentByT3.add(uuid);
      seen.add(uuid);
    }
  }
  let timeAnchor: number | undefined;
  const normalizeValue = (value: unknown): unknown => {
    if (typeof value === "string") {
      // Droid replays its session context, including the recorder's own skills, MCP servers,
      // instructions, and workspace listing. None of that belongs in a committed fixture.
      if (value.trimStart().startsWith("<system-reminder>")) {
        return "<system-reminder>redacted</system-reminder>";
      }
      let normalized = value;
      for (const workspace of workspaces)
        normalized = normalized.replaceAll(workspace, "<workspace>");
      normalized = home.length > 1 ? normalized.replaceAll(home, "~") : normalized;
      return normalized.replace(UUID_PATTERN, (uuid) => {
        const key = uuid.toLowerCase();
        if (sentByT3.has(key)) return uuid;
        const existing = uuids.get(key);
        if (existing !== undefined) return existing;
        const replacement = `00000000-0000-4000-8000-${String(uuids.size + 1).padStart(12, "0")}`;
        uuids.set(key, replacement);
        return replacement;
      });
    }
    if (Array.isArray(value)) return value.map(normalizeValue);
    if (!isRecord(value)) return value;
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key]) => key !== "_meta")
        .map(([key, entry]) => [
          key,
          key.toLowerCase() === "authorization" || key === "openaiEncryptedContent"
            ? "<redacted>"
            : key === "headers" && Array.isArray(entry)
              ? entry.filter(
                  (header) =>
                    !isRecord(header) ||
                    typeof header.name !== "string" ||
                    header.name.toLowerCase() !== "authorization",
                )
              : key === "servers" || key === "skills" || key === "mcpServers" || key === "droids"
                ? []
                : key === "availableModels" && Array.isArray(entry)
                  ? entry.filter(
                      (model) => isRecord(model) && model.id === DROID_MODEL_SELECTION.model,
                    )
                  : key === "append" && typeof entry === "string" && value.preset === "droid"
                    ? DROID_REPLAY_ANY
                    : typeof entry === "number" && /(?:at|timestamp)$/iu.test(key)
                      ? ((timeAnchor ??= entry), entry - timeAnchor + 1_767_225_600_000)
                      : normalizeValue(entry),
        ]),
    );
  };
  return entries.map((entry) =>
    entry.type === "runtime_exit" ? entry : { ...entry, frame: normalizeValue(entry.frame) },
  );
}

function encodeTranscriptNdjson(transcript: ProviderReplayTranscript): string {
  const { entries, ...metadata } = transcript;
  return [
    encodeJson({ type: "transcript_start", ...metadata }),
    ...entries.map((entry) => encodeJson(entry)),
    "",
  ].join("\n");
}

/**
 * Replay runs on a frozen TestClock; recording shares that runtime so fixture
 * `advance_clock` steps behave identically, but lets it follow wall time so a
 * live Droid's own timeouts still elapse. A TestClock timeout cannot bound the
 * run, because `advance_clock` steps would trip it at once.
 */
const followWallClock = Effect.gen(function* () {
  while (true) {
    yield* TestClock.withLive(Effect.sleep(CLOCK_TICK));
    yield* TestClock.adjust(CLOCK_TICK);
  }
});

const readDroidVersion = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const output = yield* spawner.string(ChildProcess.make(droidBinary, ["--version"]));
  return parseDroidVersion(output) ?? output.trim();
});

const record = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const version = yield* readDroidVersion;
  const outputPath = readArgValue("--out") ?? (yield* path.fromFileUrl(variant.transcriptFile));
  const fixtureInput = fixture.buildInput();
  const workspace = yield* Effect.promise(() =>
    makeCheckpointWorkspace(`droid-record-${fixture.name}`, fixtureInput.workspaceFiles),
  );
  yield* Effect.addFinalizer(() =>
    fs.remove(workspace, { recursive: true, force: true }).pipe(Effect.ignore),
  );

  const entries: Array<ProviderReplayEntry> = [];
  const placeholder: ProviderReplayTranscript = {
    provider: DROID_PROVIDER,
    protocol: DROID_STREAM_JSONRPC_PROTOCOL,
    version,
    scenario: fixture.name,
    entries: [],
  } satisfies ProviderReplayTranscript;
  const recordingSpawner = Layer.effect(
    ChildProcessSpawner.ChildProcessSpawner,
    Effect.map(Effect.service(ChildProcessSpawner.ChildProcessSpawner), (live) =>
      makeDroidRecordingSpawner(live, entries),
    ),
  ).pipe(Layer.provide(NodeServices.layer));
  const materializeInput = materializeFixtureInput({
    scenario: fixture.name,
    fixtureInput,
    driver: DROID_PROVIDER,
    modelSelection: DROID_MODEL_SELECTION,
  }).pipe(Effect.provide(IdAllocator.layer));
  const continuationOptions =
    variant.runContinuationWorker === true ? { runContinuationWorker: true } : {};

  yield* Effect.gen(function* () {
    yield* followWallClock.pipe(Effect.forkScoped);
    const materialized = yield* materializeInput;
    yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${fixture.name}/droid:record`,
        transcript: placeholder,
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: workspace },
      },
      {
        driver: DROID_PROVIDER,
        decodeTranscript: (transcript) => Effect.succeed(transcript),
        makeProviderAdapterRegistryLayer: () =>
          droidReplayLayer({
            binaryPath: droidBinary,
            environment: makeDroidEnvironment(process.env),
            spawner: recordingSpawner,
          }),
      },
      continuationOptions,
    );
  }).pipe(Effect.scoped, provideDeterministicTestRuntime);

  const transcript = {
    ...placeholder,
    metadata: {
      generatedBy: "live-droid-recorder",
      model: DROID_MODEL_SELECTION.model,
    },
    entries: normalizeEntries(entries, [yield* fs.realPath(workspace), workspace]),
  } satisfies ProviderReplayTranscript;
  yield* Effect.gen(function* () {
    const replayWorkspace = yield* checkpointWorkspace(fixture.name, fixtureInput.workspaceFiles);
    const materialized = yield* materializeInput;
    const replayResult = yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${fixture.name}/droid:verify`,
        transcript: yield* DroidOrchestratorReplayHarness.decodeTranscript(
          materializeReplayTranscriptWorkspace(transcript, yield* fs.realPath(replayWorkspace)),
        ),
        commands: materialized.commands,
        steps: materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...variant.runtimePolicyOverride, cwd: replayWorkspace },
      },
      DroidOrchestratorReplayHarness,
      continuationOptions,
    );
    variant.assertOutput(replayResult, transcript);
  }).pipe(Effect.scoped, provideDeterministicTestRuntime);

  yield* fs.makeDirectory(path.dirname(outputPath), { recursive: true });
  yield* fs.writeFileString(outputPath, encodeTranscriptNdjson(transcript));
  yield* Console.log(`Wrote ${transcript.entries.length} Droid JSON-RPC entries to ${outputPath}`);
});

await Effect.runPromise(record.pipe(Effect.scoped, Effect.provide(NodeServices.layer)));
