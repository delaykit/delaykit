import { PostgresStore } from "../src/stores/postgres.js";
import { storeContractSuite } from "./store-contract.js";
import { TEST_URL, truncatePostgresJobs } from "./helpers/postgres-fixture.js";

storeContractSuite(
  "PostgresStore",
  () => PostgresStore.connect(TEST_URL),
  truncatePostgresJobs,
);
