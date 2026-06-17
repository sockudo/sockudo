# Sessions

A session is one Sockudo channel carrying AI lifecycle events and mutable message state. Clients
subscribe through `@sockudo/client`, then `@sockudo/ai-transport` folds serial-ordered channel
events into the tree and current view.

Use private or presence channels when identity, cancellation ownership, presence status, or history
capability is required.
