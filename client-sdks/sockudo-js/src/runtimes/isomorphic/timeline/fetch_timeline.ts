import Logger from "core/logger";
import TimelineSender from "core/timeline/timeline_sender";
import * as Collections from "core/utils/collections";

const getAgent = (sender: TimelineSender, useTLS: boolean) => {
  return (data: unknown, _callback: (...args: any[]) => any) => {
    const scheme = `http${useTLS ? "s" : ""}://`;
    let url =
      scheme + (sender.host || sender.options.host) + sender.options.path;
    const query = Collections.buildQueryString(data);
    url += `/${2}?${query}`;

    void fetch(url)
      .then(async (response) => {
        if (response.status !== 200) {
          throw new Error(`received ${response.status} from stats endpoint`);
        }
        return response.json() as Promise<{ host?: string }>;
      })
      .then(({ host }) => {
        if (host) {
          sender.host = host;
        }
      })
      .catch((error: unknown) => {
        Logger.debug("TimelineSender Error: ", error);
      });
  };
};

const fetchTimeline = {
  name: "fetch",
  getAgent,
};

export default fetchTimeline;
