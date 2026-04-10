import { MemoryStore } from "../src/stores/memory.js";
import { storeContractSuite } from "./store-contract.js";

storeContractSuite("MemoryStore", async () => new MemoryStore());
