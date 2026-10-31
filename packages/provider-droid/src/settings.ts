/**
 * Factory Droid instance settings shared by the server and client.
 *
 * @module provider-droid/settings
 */
import {
  CustomModelSetting,
  makeBinaryPathSetting,
  makeProviderSettingsSchema,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const DroidSettings = makeProviderSettingsSchema(
  {
    enabled: Schema.Boolean.pipe(
      Schema.withDecodingDefault(Effect.succeed(true)),
      Schema.annotateKey({ providerSettingsForm: { hidden: true } }),
    ),
    binaryPath: makeBinaryPathSetting("droid").pipe(
      Schema.annotateKey({
        title: "Binary path",
        description: "Path to the Factory Droid executable on this environment.",
        providerSettingsForm: { placeholder: "droid", clearWhenEmpty: "omit" },
      }),
    ),
    toolExecutionMode: Schema.optionalKey(
      Schema.Literals(["default", "direct_only", "script_only"]),
    ).pipe(
      Schema.annotateKey({
        title: "Tool execution mode",
        description: "Applies to new sessions only. Default lets Droid choose for the model.",
        providerSettingsForm: {
          control: "select",
          options: [
            { value: "default", label: "Default (Droid decides)" },
            { value: "direct_only", label: "Direct tools only" },
            { value: "script_only", label: "Script only" },
          ],
        },
      }),
    ),
    customModels: Schema.Array(CustomModelSetting).pipe(
      Schema.withDecodingDefault(Effect.succeed([])),
      Schema.annotateKey({ providerSettingsForm: { hidden: true } }),
    ),
  },
  { order: ["binaryPath", "toolExecutionMode"] },
);
export type DroidSettings = typeof DroidSettings.Type;
