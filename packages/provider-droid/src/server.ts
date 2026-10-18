/**
 * Factory Droid server integration and adapter factory for replay testkits.
 *
 * @module provider-droid/server
 */
export { DroidDriver, type DroidDriverEnv } from "./server/driver.ts";
export {
  DROID_PROVIDER,
  DroidAdapterV2Driver,
  type DroidAdapterV2DriverEnv,
} from "./server/adapter.ts";
