import { TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { ProviderTextGeneration } from "@t3tools/provider-core/server/textGeneration";

/** Match ACP's explicit refusal rather than spawning a tool-capable coding session for app copy. */
export function makeDroidTextGeneration(): ProviderTextGeneration {
  const unsupported = (operation: string) =>
    Effect.fail(
      new TextGenerationError({
        operation,
        detail: "Native Droid does not provide application text generation yet.",
      }),
    );
  return {
    generateCommitMessage: () => unsupported("generateCommitMessage"),
    generatePrContent: () => unsupported("generatePrContent"),
    generateBranchName: () => unsupported("generateBranchName"),
    generateThreadTitle: () => unsupported("generateThreadTitle"),
    generateReviewGuide: () => unsupported("generateReviewGuide"),
  };
}
