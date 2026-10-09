import { expect, it } from "vite-plus/test";
import { DroidSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { getProviderClient } from "./providerClients";

const decodeSettings = Schema.decodeSync(DroidSettings);

it("exposes native Droid settings without rewriting provider-owned custom ids", () => {
  const client = getProviderClient("droid");
  expect(client?.driverKind).toBe(ProviderDriverKind.make("droid"));
  expect(client?.label).toBe("Factory Droid (native)");
  expect(client?.settingsSchema).toBe(DroidSettings);
  expect(
    decodeSettings({
      binaryPath: "/custom/droid",
      customModels: ["custom:GPT-6 Luna-12"],
    }),
  ).toMatchObject({ binaryPath: "/custom/droid", customModels: ["custom:GPT-6 Luna-12"] });
});
