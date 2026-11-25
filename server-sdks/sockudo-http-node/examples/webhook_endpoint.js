const express = require("express");
const Sockudo = require("../dist/sockudo");

const sockudo = new Sockudo({
  appId: process.env["SOCKUDO_APP_ID"],
  key: process.env["SOCKUDO_KEY"],
  secret: process.env["SOCKUDO_SECRET"],
  host: process.env["SOCKUDO_HOST"] || "127.0.0.1",
  port: process.env["SOCKUDO_PORT"] || 6001,
});

const app = express();

app.use(function (req, _res, next) {
  req.rawBody = "";
  req.setEncoding("utf8");

  req.on("data", function (chunk) {
    req.rawBody += chunk;
  });

  req.on("end", function () {
    next();
  });
});

app.post("/webhook", function (req, res) {
  const webhook = sockudo.webhook(req);

  if (!webhook.isValid()) {
    res.status(401).send("Invalid signature");
    return;
  }

  console.log("data:", webhook.getData());
  console.log("events:", webhook.getEvents());
  console.log("time:", webhook.getTime());
  res.send("OK");
});

if (require.main === module) {
  const port = Number(process.env["PORT"] || 3000);
  app.listen(port, function () {
    console.log("webhook endpoint listening on http://127.0.0.1:" + port + "/webhook");
  });
}

module.exports = app;
