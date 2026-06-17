# Token Streaming

Streaming output starts as `sockudo:message.create`, continues through `message.append`, and
terminates with `status=complete` or `status=cancelled`. Append rollup is egress-only; history and
recovery still see the full version chain.
