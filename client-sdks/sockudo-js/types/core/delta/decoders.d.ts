declare function base64ToBytes(base64: string): Uint8Array;
declare function bytesToString(bytes: Uint8Array | number[]): string;
declare function stringToBytes(str: string): Uint8Array;
export declare class FossilDeltaDecoder {
    static isAvailable(): boolean;
    static apply(base: string | Uint8Array, delta: string | Uint8Array): string;
}
export declare class Xdelta3Decoder {
    static isAvailable(): boolean;
    static apply(base: string | Uint8Array, delta: string | Uint8Array): string;
}
export { base64ToBytes, bytesToString, stringToBytes };
