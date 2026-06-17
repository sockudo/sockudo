import Strategy from "./strategy";
import StrategyRunner from "./strategy_runner";

/** Proxies method calls to one of substrategies basing on the test function.
 *
 * @param {(...args: any[]) => any} test
 * @param {Strategy} trueBranch strategy used when test returns true
 * @param {Strategy} falseBranch strategy used when test returns false
 */
export default class IfStrategy implements Strategy {
  test: () => boolean;
  trueBranch: Strategy;
  falseBranch: Strategy;

  constructor(
    test: () => boolean,
    trueBranch: Strategy,
    falseBranch: Strategy,
  ) {
    this.test = test;
    this.trueBranch = trueBranch;
    this.falseBranch = falseBranch;
  }

  isSupported(): boolean {
    const branch = this.test() ? this.trueBranch : this.falseBranch;
    return branch.isSupported();
  }

  connect(
    minPriority: number,
    callback: (...args: any[]) => any,
  ): StrategyRunner {
    const branch = this.test() ? this.trueBranch : this.falseBranch;
    return branch.connect(minPriority, callback);
  }
}
