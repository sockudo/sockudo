interface Callback {
  fn: (...args: any[]) => any;
  context: unknown;
}

export default Callback;
