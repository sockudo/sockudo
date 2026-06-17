interface ScriptReceiver {
  number: number;
  id: string;
  name: string;
  callback: (...args: any[]) => any;
}

export default ScriptReceiver;
