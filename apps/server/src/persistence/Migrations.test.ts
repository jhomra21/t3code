import { assert, describe, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationEntries, migrationManifest, runMigrations } from "./Migrations.ts";
import * as Migrator from "effect/sql/Migrator";

const v2Migrations = migrationManifest.filter(([id]) => id >= 55);
const newerMigrationId = Math.max(...migrationManifest.map(([id]) => id)) + 1;

const forkNames = [
  "PeerLinks",
  "PeerLinkFence",
  "CanonicalizeThreadPullRequestLinks",
  "CodexUsageNativeTurnIndex",
  "RepairClientSettlementTimestamps",
  "WebhookDedupe",
  "ShellStopRequestIndex",
  "ThreadMessageSearch",
];

const seedFork = (through: number) =>
  Effect.gen(function* () {
    yield* runMigrations({ toMigrationInclusive: 63 });
    const loader = Object.fromEntries(
      forkNames.slice(0, through - 63).map((name, index) => {
        const entry = migrationEntries.find(([, candidate]) => candidate === name);
        if (entry === undefined) throw new Error(`Missing migration ${name}`);
        return [`${64 + index}_${name}`, entry[2]];
      }),
    );
    yield* Migrator.make({})({ loader: Migrator.fromRecord(loader) });
  });

const history = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
  `;
  return rows.map((row) => [row.migration_id, row.name] as const);
});

const v2TableCount = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'orchestration_v2_%'
  `;
  return rows.length;
});

describe("runMigrations", () => {
  it.effect.each([64, 65, 66, 67, 68, 69, 70, 71])(
    "upgrades fork peer history through %s without losing rows or replaying schema",
    (through) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* seedFork(through);
        yield* sql`UPDATE effect_sql_migrations SET created_at = '2026-10-27 00:00:00' WHERE migration_id = 64`;
        yield* sql`INSERT INTO peer_environment_links
        (environment_id, secret_name, label, urls_json, access, linked_at, expires_at)
        VALUES ('test-peer', 'test-secret-reference', 'Peer', '[]', 'auto', '2026-10-27', '2026-11-27')`;
        const rows =
          yield* sql`SELECT environment_id, secret_name, label FROM peer_environment_links`;
        yield* runMigrations();
        assert.deepStrictEqual(yield* history, migrationManifest);
        assert.deepStrictEqual(
          yield* sql`SELECT environment_id, secret_name, label FROM peer_environment_links`,
          rows,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT created_at FROM effect_sql_migrations WHERE migration_id = 70`,
          [{ created_at: "2026-10-27 00:00:00" }],
        );
        assert.deepStrictEqual(yield* runMigrations(), []);
        assert.strictEqual(
          (yield* sql<{ name: string }>`PRAGMA table_info(peer_environment_links)`).filter(
            (column) => column.name === "fenced",
          ).length,
          1,
        );
        assert.strictEqual(
          (yield* sql`SELECT name FROM sqlite_master WHERE name = 'orchestration_v2_projection_turn_items_waiting_link_idx'`)
            .length,
          1,
        );
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect.each(["unknown", "gap", "schema", "early"])(
    "refuses unsafe fork history: %s",
    (failure) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* seedFork(71);
        if (failure === "unknown")
          yield* sql`INSERT INTO effect_sql_migrations (migration_id, name) VALUES (72, 'UnknownFork')`;
        if (failure === "gap")
          yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id = 68`;
        if (failure === "schema") yield* sql`ALTER TABLE peer_environment_links DROP COLUMN fenced`;
        const before = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
        const schema = yield* sql`SELECT sql FROM sqlite_master ORDER BY name`;
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              runMigrations(failure === "early" ? { toMigrationInclusive: 70 } : {}),
            ),
          ),
        );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
          before,
        );
        assert.deepStrictEqual(yield* sql`SELECT sql FROM sqlite_master ORDER BY name`, schema);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("rolls back a failed fork ledger rewrite and retries without duplicate schema", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* seedFork(64);
      yield* sql`CREATE TRIGGER refuse_fork_rewrite BEFORE UPDATE ON effect_sql_migrations
        BEGIN SELECT RAISE(ABORT, 'injected rewrite failure'); END`;
      const before = yield* history;
      const schema = yield* sql`SELECT sql FROM sqlite_master ORDER BY name`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(runMigrations())));
      assert.deepStrictEqual(yield* history, before);
      assert.deepStrictEqual(yield* sql`SELECT sql FROM sqlite_master ORDER BY name`, schema);
      yield* sql`DROP TRIGGER refuse_fork_rewrite`;
      yield* runMigrations();
      assert.deepStrictEqual(yield* history, migrationManifest);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
  it.effect("refuses to start when another migration took a V2 id", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name) VALUES (55, 'FutureMainMigration')
      `;
      const before = yield* history;

      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 54 }), []);
      const exit = yield* Effect.exit(runMigrations());

      assert.isTrue(Exit.isFailure(exit));
      assert.include(String(exit), "55_OrchestrationV2 (recorded: FutureMainMigration)");
      assert.deepStrictEqual(yield* history, before);
      assert.strictEqual(yield* v2TableCount, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("refuses to start when a later id would skip V2 migrations", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES (${newerMigrationId}, 'FutureMainMigration')
      `;
      const before = yield* history;

      const exit = yield* Effect.exit(runMigrations());

      assert.isTrue(Exit.isFailure(exit));
      for (const [id, name] of v2Migrations) {
        assert.include(String(exit), `${id}_${name} (recorded: nothing)`);
      }
      assert.deepStrictEqual(yield* history, before);
      assert.strictEqual(yield* v2TableCount, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("upgrades a database at exactly 54", () =>
    Effect.gen(function* () {
      yield* runMigrations({ toMigrationInclusive: 54 });
      assert.deepStrictEqual(yield* runMigrations(), v2Migrations);
      assert.deepStrictEqual(yield* history, migrationManifest);
      assert.isAbove(yield* v2TableCount, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect.each([30, 54])("still starts with a site-local migration at id %s", (id) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* sql`
        UPDATE effect_sql_migrations SET name = 'SiteLocalMigration' WHERE migration_id = ${id}
      `;

      assert.deepStrictEqual(yield* runMigrations(), v2Migrations);
      assert.isAbove(yield* v2TableCount, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("only checks V2 migrations within the requested limit", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 55 });
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES (56, 'SiteLocalMigration'), (${newerMigrationId}, 'NewerV2Migration')
      `;
      const before = yield* history;

      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 55 }), []);
      const exit = yield* Effect.exit(runMigrations({ toMigrationInclusive: 56 }));
      assert.isTrue(Exit.isFailure(exit));
      assert.include(
        String(exit),
        "56_RemoveRedundantProjectionIndexes (recorded: SiteLocalMigration)",
      );
      assert.notInclude(String(exit), "57_ScheduledTaskWebhooks");
      assert.deepStrictEqual(yield* history, before);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("still starts after a newer build recorded a later V2 migration", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES (${newerMigrationId}, 'NewerV2Migration')
      `;

      assert.deepStrictEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});
