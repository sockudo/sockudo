import Sockudo from './sockudo';
import { Options } from './options';
export default class SockudoWithEncryption extends Sockudo {
    constructor(app_key: string, options: Options);
}
