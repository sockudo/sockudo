/*
Package sockudo is the Golang library for interacting with the Sockudo HTTP API.

This package lets you trigger events to your client and query the state
of your Sockudo channels. When used with a server, you can validate Sockudo
webhooks and authenticate private- or presence-channels.

In order to use this library, you need to have a Sockudo instance running.
You will need the application credentials for your app.

# Getting Started

To create a new client, pass in your application credentials to a `sockudo.Client` struct:

	sockudoClient := sockudo.Client{
		AppID: "your_app_id",
		Key: "your_app_key",
		Secret: "your_app_secret",
	}

To start triggering events on a channel, we call `sockudoClient.Trigger`:

	data := map[string]string{"message": "hello world"}

	// trigger an event on a channel, along with a data payload
	sockudoClient.Trigger("test_channel", "event", data)

Read on to see what more you can do with this library, such as
authenticating private- and presence-channels, validating Sockudo webhooks,
and querying the HTTP API to get information about your channels.

Author: Jamie Patel, Sockudo
*/
package sockudo
