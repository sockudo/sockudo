import { Agent } from "https";

import * as Sockudo from "sockudo";

const sockudo = Sockudo.forURL(process.env.SOCKUDO_URL, {
  encryptionMasterKeyBase64: Buffer.from(
    "01234567890123456789012345678901",
  ).toString("base64"),
  agent: new Agent({ keepAlive: true }),
});

sockudo
  .get({
    path: "/channels",
    params: { filter_by_prefix: "presence-" },
  })
  .then((response: Sockudo.Response) => {
    console.log(`received response with status ${response.status}`);
    return response.text().then((body) => {
      console.log(`and body ${body}`);
    });
  })
  .catch((err) => {
    console.log(`received error ${err}`);
  });

const _authResponse: Sockudo.AuthResponse = sockudo.authenticate(
  "123.456",
  "private-encrypted-example",
  {
    user_id: "foo",
    user_info: { bar: 42 },
  },
);
