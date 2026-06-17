# Codec

Codecs map application messages to Sockudo AI Transport operations. The transport treats
`extras.ai.codec` as opaque bounded string headers and uses `extras.ai.transport` only for frozen
transport keys.

For Vercel UI messages, use `UIMessageCodec`. For custom domains, build on `createEncoderCore`,
`createDecoderCore`, and `createLifecycleTracker`.
