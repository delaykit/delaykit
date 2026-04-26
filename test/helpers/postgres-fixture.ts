import type postgres from "postgres";
import type { Store } from "../../src/types.js";

export const TEST_URL =
  "postgres://delaykit:delaykit@localhost:5444/delaykit_test";

function rawSql(store: Store): postgres.Sql {
  return (store as unknown as { sql: postgres.Sql }).sql;
}

export async function truncatePostgresJobs(store: Store): Promise<void> {
  await rawSql(store)`DELETE FROM delaykit.jobs`;
}

export async function dropPostgresSchema(store: Store): Promise<void> {
  await rawSql(store)`DROP SCHEMA IF EXISTS delaykit CASCADE`;
}
