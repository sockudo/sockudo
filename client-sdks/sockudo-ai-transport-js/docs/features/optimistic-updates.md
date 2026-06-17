# Optimistic Updates

Clients insert optimistic nodes before the HTTP poke returns. Server acks reconcile by codec message
id; POST failures leave the node visible and emit an SDK error.
