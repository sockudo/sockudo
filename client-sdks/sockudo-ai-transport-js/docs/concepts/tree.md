# Conversation Tree

The tree records messages, turns, forks, edits, regeneration, and sibling selection. `View`
materializes one branch at a time while `createView()` allows comparison panes over the same
underlying session.

`TMessage.id` is the codec message id and equals Sockudo `message_serial` for streamed mutable
messages.
