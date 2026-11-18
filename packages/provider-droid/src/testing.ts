/**
 * Native Droid protocol helpers used by replay testkits and fixture recorders.
 *
 * @module provider-droid/testing
 */
export { makeDroidEnvironment } from "./server/status.ts";
export { parseDroidVersion } from "./server/maintenance.ts";
export { DROID_PROVIDER } from "./server/adapter.ts";
export { makeDroidAdapterV2, type DroidAdapterV2Options } from "./server/adapter.ts";
