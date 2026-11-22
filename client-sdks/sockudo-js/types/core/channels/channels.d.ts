import Channel from './channel';
import ChannelTable from './channel_table';
import Sockudo from '../sockudo';
export default class Channels {
    channels: ChannelTable;
    constructor();
    add(name: string, pusher: Sockudo): Channel;
    all(): Channel[];
    find(name: string): Channel;
    remove(name: string): Channel;
    disconnect(): void;
}
