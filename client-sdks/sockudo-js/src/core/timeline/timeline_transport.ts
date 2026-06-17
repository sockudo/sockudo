import TimelineSender from "../timeline/timeline_sender";

interface TimelineTransport {
  name: string;
  getAgent: (
    sender: TimelineSender,
    useTLS: boolean,
  ) => (data: any, callback: (...args: any[]) => any) => void;
}

export default TimelineTransport;
