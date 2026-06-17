import Sockudo from "./sockudo";
import { Options, validateOptions } from "./options";
import * as nacl from "tweetnacl";

export default class SockudoWithEncryption extends Sockudo {
  constructor(app_key: string, options: Options) {
    Sockudo.logToConsole = SockudoWithEncryption.logToConsole;
    Sockudo.log = SockudoWithEncryption.log;

    validateOptions(options);
    options.nacl = nacl;
    super(app_key, options);
  }
}
