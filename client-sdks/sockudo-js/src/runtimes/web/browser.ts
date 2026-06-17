import AbstractRuntime from "runtimes/interface";
import Ajax from "core/http/ajax";

interface Browser extends AbstractRuntime {
  onDocumentBody(callback: (...args: any[]) => any);
  getDocument(): Document;
  createXMLHttpRequest(): Ajax;
}

export default Browser;
