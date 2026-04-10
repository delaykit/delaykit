import { PostgresStore } from "../src/stores/postgres.js";
import { storeContractSuite } from "./store-contract.js";

const TEST_URL = "postgres://delaykit:delaykit@localhost:5444/delaykit_test";

storeContractSuite(
  "PostgresStore",
  () => PostgresStore.connect(TEST_URL),
  async (store) => {
    await (store as any).sql`DELETE FROM delaykit.jobs`;
  },
);
