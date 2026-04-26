import { SQLiteStore } from "../src/stores/sqlite.js";
import { storeContractSuite } from "./store-contract.js";
import { truncateSqliteJobs } from "./helpers/sqlite-fixture.js";

storeContractSuite(
  "SQLiteStore",
  () => SQLiteStore.connect(":memory:"),
  async (store) => truncateSqliteJobs(store),
);
