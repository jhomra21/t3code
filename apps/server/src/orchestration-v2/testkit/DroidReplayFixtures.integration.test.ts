import { describe } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import { materializeReplayTranscriptWorkspace } from "@t3tools/provider-testing/replayTranscript";
import { DroidOrchestratorReplayHarness } from "../Adapters/DroidAdapterV2.testkit.ts";
import { itRunsReplayFixtures, runReplayFixture } from "./ProviderReplayFixtures.ts";

describe("Droid replay fixtures", () => {
  itRunsReplayFixtures("droid", (input) =>
    runReplayFixture({
      ...input,
      harness: DroidOrchestratorReplayHarness,
      materializeWorkspace: (transcript, workspace) =>
        FileSystem.FileSystem.pipe(
          Effect.flatMap((fs) => fs.realPath(workspace)),
          Effect.map((realPath) => materializeReplayTranscriptWorkspace(transcript, realPath)),
          Effect.provide(NodeServices.layer),
        ),
    }),
  );
});
