import { describe, expect, it } from "vitest";
import { Filter, validateFilter } from "../src/filter";

describe("full port smoke", () => {
  it("exports Filter helpers", () => {
    expect(typeof Filter.and).toBe("function");

    const node = Filter.and({ key: "a", cmp: "eq", val: "1" });
    expect(validateFilter(node)).toBeNull();
  });
});
