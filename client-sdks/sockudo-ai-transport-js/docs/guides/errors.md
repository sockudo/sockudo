# Errors

All async SDK public methods reject with `ErrorInfo`; synchronous misuse throws `ErrorInfo`.

| Code   | Name                            | Recovery                                               |
| ------ | ------------------------------- | ------------------------------------------------------ |
| 104000 | `ai_invalid_transport_header`   | Fix frozen transport header names or values.           |
| 104001 | `ai_header_too_large`           | Reduce key count, key length, or value length.         |
| 104002 | `ai_client_id_spoof`            | Use the server-verified client identity.               |
| 104003 | `ai_event_not_permitted`        | Publish agent events with app key or agent capability. |
| 104004 | `ai_rollup_window_invalid`      | Use `0`, `20`, `40`, `100`, or `500`.                  |
| 104005 | `ai_until_attach_gap`           | Resubscribe and retry history.                         |
| 104006 | `ai_op_duplicate_conflict`      | Use a new `op_id`.                                     |
| 104007 | `ai_stream_limit_exceeded`      | Start a new message or reduce output.                  |
| 104008 | `ai_capability_pattern_invalid` | Fix capability token channel patterns.                 |
| 104009 | `ai_presence_update_invalid`    | Fix presence update payload.                           |
| 104010 | `ai_reserved`                   | Follow the error message.                              |
| 93002  | `mutable_not_permitted`         | Enable mutable messages on the channel namespace.      |
