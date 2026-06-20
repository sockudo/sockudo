declare module "@react-native-community/netinfo" {
  export interface NetInfoState {
    type: string;
  }

  export interface NetInfoModule {
    fetch(): Promise<NetInfoState>;
    addEventListener(listener: (state: NetInfoState) => void): () => void;
  }

  const NetInfo: NetInfoModule;
  export default NetInfo;
}
