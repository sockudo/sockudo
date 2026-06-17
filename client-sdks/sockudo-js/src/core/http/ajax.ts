interface Ajax {
  open(
    method: string,
    url: string,
    async?: boolean,
    user?: string,
    password?: string,
  ): void;
  send(payload?: any): void;
  setRequestHeader(key: string, value: string): void;
  onreadystatechange: (...args: any[]) => any;
  readyState: number;
  responseText: string;
  status: number;
  withCredentials?: boolean;

  ontimeout: (...args: any[]) => any;
  onerror: (...args: any[]) => any;
  onprogress: (...args: any[]) => any;
  onload: (...args: any[]) => any;
  abort: (...args: any[]) => any;
}

export default Ajax;
