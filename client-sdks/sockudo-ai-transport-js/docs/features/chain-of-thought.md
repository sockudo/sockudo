# Chain Of Thought

Reasoning parts are codec data, not transport data. Keep provider-specific reasoning headers under
`extras.ai.codec` and avoid logging payloads or hidden reasoning tokens.
