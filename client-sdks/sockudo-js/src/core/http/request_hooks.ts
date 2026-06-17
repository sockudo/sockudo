import HTTPRequest from "./http_request";
import Ajax from "./ajax";

interface RequestHooks {
  getRequest(request: HTTPRequest): Ajax;
  abortRequest(request: Ajax): void;
}

export default RequestHooks;
