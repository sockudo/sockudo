const express = require("express");
const Sockudo = require("../dist/sockudo");

// provide auth details via the SOCKUDO_URL environment variable
const sockudo = new Sockudo({ url: process.env["SOCKUDO_URL"] });

const app = express();
app.use(function (req, res, next) {
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
  console.log("data:", webhook.getData());
  console.log("events:", webhook.getEvents());
  console.log("time:", webhook.getTime());
  console.log("valid:", webhook.isValid());
  res.send("OK");
});

app.listen(3000);
