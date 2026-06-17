import AbstractRuntime from "../../runtimes/interface";
import { AuthRequestType, InternalAuthOptions } from "./options";

interface AuthTransport {
  (
    context: AbstractRuntime,
    query: string,
    authOptions: InternalAuthOptions,
    authRequestType: AuthRequestType,
    callback: (...args: any[]) => any,
  ): void;
}

interface AuthTransports {
  [index: string]: AuthTransport;
}

export { AuthTransport, AuthTransports };
