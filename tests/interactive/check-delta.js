// check-delta.js
import { applyDelta } from "fossil-delta";

const base = Buffer.from(
  '{"event":"price","channel":"market-data","data":{"asset":"BTC"}}',
);
const deltaB64 = "MTAKMTBAMCwyek95V0Q7"; // from the server payload
const deltaBytes = Buffer.from(deltaB64, "base64");

const out = applyDelta(base, deltaBytes);
console.log(out.toString());
