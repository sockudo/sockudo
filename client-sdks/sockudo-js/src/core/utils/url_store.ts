/**
 * A place to store help URLs for error messages etc
 */

const urlStore = {
  baseUrl: "https://docs.sockudo.io",
  urls: {
    authenticationEndpoint: {
      path: "/server/security#authentication",
    },
    authorizationEndpoint: {
      path: "/server/security#authorization",
    },
    javascriptQuickStart: {
      path: "/getting-started/first-connection",
    },
    triggeringClientEvents: {
      path: "/client/usage#client-events",
    },
    encryptedChannelSupport: {
      fullUrl:
        "https://github.com/sockudo/sockudo-js#encrypted-channel-support",
    },
  },
};

/** Builds a consistent string with links to Sockudo documentation
 *
 * @param {string} key - relevant key in the url_store.urls object
 * @return {string} suffix string to append to log message
 */
const buildLogSuffix = function (key: string): string {
  const urlPrefix = "See:";
  const urlObj = urlStore.urls[key];
  if (!urlObj) return "";

  let url;
  if (urlObj.fullUrl) {
    url = urlObj.fullUrl;
  } else if (urlObj.path) {
    url = urlStore.baseUrl + urlObj.path;
  }

  if (!url) return "";
  return `${urlPrefix} ${url}`;
};

export default { buildLogSuffix };
