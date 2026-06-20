# sockudo

Official Ruby server SDK for [Sockudo](https://github.com/sockudo/sockudo) — a fast, self-hosted WebSocket server with full Pusher HTTP API compatibility.

## Supported Platforms

- **Ruby 3.0 or greater**
- Rails and other Ruby frameworks are supported provided you are running a supported Ruby version.

## Installation

Add to your Gemfile:

```ruby
gem 'sockudo'
```

Then run `bundle install`.

For local monorepo development, use:

```ruby
gem 'sockudo', path: '../sockudo/server-sdks/sockudo-http-ruby'
```

## Getting Started

```ruby
require 'sockudo'

sockudo = Sockudo::Client.new(
  app_id: 'your-app-id',
  key:    'your-app-key',
  secret: 'your-app-secret',
  host:   '127.0.0.1',
  port:    6001,
  use_tls: false
)

sockudo.trigger('my-channel', 'my-event', { message: 'hello world' })
```

## Configuration

### Instance Configuration

```ruby
sockudo = Sockudo::Client.new(
  app_id:  'your-app-id',
  key:     'your-app-key',
  secret:  'your-app-secret',
  host:    '127.0.0.1',
  port:    6001,
  use_tls: false
)
```

`use_tls` is optional and defaults to `true`. It sets the scheme and port accordingly. A custom `port` value takes precedence over `use_tls`.

### Global Configuration

```ruby
Sockudo.app_id  = 'your-app-id'
Sockudo.key     = 'your-app-key'
Sockudo.secret  = 'your-app-secret'
Sockudo.host    = '127.0.0.1'
Sockudo.port    = 6001
Sockudo.use_tls = false
```

### From Environment Variable

If `SOCKUDO_URL` is set in the environment, use `from_env` to configure automatically:

```ruby
# Reads SOCKUDO_URL in the form: http://KEY:SECRET@HOST:PORT/apps/APP_ID
sockudo = Sockudo::Client.from_env
```

Global configuration is also automatically read from `SOCKUDO_URL` when set.

### HTTP Proxy

```ruby
Sockudo.http_proxy = 'http://user:password@proxy-host:8080'
```

## Publishing Events

### Single Channel

```ruby
sockudo.trigger('my-channel', 'my-event', { message: 'hello world' })
```

### Multiple Channels

```ruby
sockudo.trigger(['channel-1', 'channel-2'], 'my-event', { message: 'hello world' })
```

Up to 10 channels per call.

### Excluding a Socket Recipient

Pass a `socket_id` option to prevent the triggering connection from receiving its own event:

```ruby
sockudo.trigger('my-channel', 'my-event', { message: 'hello' }, { socket_id: '123.456' })
```

### Batch Events

Send multiple events in a single HTTP request (up to 10 per call):

```ruby
sockudo.trigger_batch([
  { channel: 'channel-1', name: 'event-1', data: { x: 1 } },
  { channel: 'channel-2', name: 'event-2', data: { x: 2 } },
  { channel: 'channel-3', name: 'event-3', data: { x: 3 } },
])
```

## Idempotent Publishing

Pass an `idempotency_key` to safely retry publishes without causing duplicate deliveries:

```ruby
sockudo.trigger(
  'my-channel',
  'my-event',
  { message: 'hello' },
  { idempotency_key: 'order-shipped-order-789' }
)
```

The server deduplicates publishes with the same key within the configured window.

## Channel Authorization

### Private Channel

```ruby
auth = sockudo.authenticate('private-my-channel', params[:socket_id])
# Returns JSON: {"auth":"key:signature"}
```

### Presence Channel

```ruby
auth = sockudo.authenticate(
  'presence-my-channel',
  params[:socket_id],
  user_id: 'user-123',
  user_info: { name: 'Jane Doe', role: 'admin' }
)
# Returns JSON: {"auth":"key:signature","channel_data":"..."}
```

## User Authentication

```ruby
auth = sockudo.authenticate_user(params[:socket_id], { id: 'user-123', name: 'Jane Doe' })
```

## Webhooks

Create a webhook object from a `Rack::Request` (available as `request` in Rails controllers and Sinatra handlers):

```ruby
webhook = sockudo.webhook(request)

if webhook.valid?
  webhook.events.each do |event|
    case event['name']
    when 'channel_occupied'
      puts "Channel occupied: #{event['channel']}"
    when 'channel_vacated'
      puts "Channel vacated: #{event['channel']}"
    when 'client_event'
      puts "Client event: #{event['event']} on #{event['channel']}"
    end
  end

  render plain: 'ok'
else
  render plain: 'invalid', status: 401
end
```

## Application State

```ruby
# Info about a channel
info = sockudo.channel_info('my-channel')
occupied = info[:occupied]

# User count for a presence channel
info = sockudo.channel_info('presence-my-channel', info: 'user_count')
user_count = info[:user_count]

# List channels (optionally filtered)
result = sockudo.channels(filter_by_prefix: 'presence-')

# Read channel history with newest-first pagination
page = sockudo.channel_history('my-channel', limit: 50, direction: 'newest_first')
next_cursor = page[:next_cursor]

# Continue pagination with an opaque cursor
page_2 = sockudo.channel_history('my-channel', cursor: next_cursor)

# Read presence history for a presence channel
presence_page = sockudo.channel_presence_history(
  'presence-my-channel',
  limit: 50,
  direction: 'newest_first'
)

# Reconstruct effective members at a point in time
snapshot = sockudo.channel_presence_snapshot(
  'presence-my-channel',
  at_serial: 4
)

# Users in a presence channel
result = sockudo.channel_users('presence-my-channel')
```

## Async Requests

There are two reasons to use async methods: avoiding blocking in a request-response cycle, or running inside an event loop.

### With EventMachine

Add `em-http-request` to your Gemfile and run with the EventMachine reactor active (e.g. inside Thin):

```ruby
sockudo.get_async('/channels').callback { |response|
  puts response[:channels].inspect
}.errback { |error|
  puts "Error: #{error}"
}

sockudo.trigger_async('my-channel', 'my-event', { message: 'hello' }).callback { |response|
  puts 'Triggered'
}
```

### Without EventMachine (Threaded)

If the EventMachine reactor is not running, async requests are made using threads managed by `httpclient`. An `HTTPClient::Connection` object is returned immediately.

```ruby
sockudo.trigger_async('my-channel', 'my-event', { message: 'hello' })
```

## Error Handling

All errors are descendants of `Sockudo::Error`:

```ruby
begin
  sockudo.trigger('my-channel', 'my-event', { message: 'hello' })
rescue Sockudo::AuthenticationError => e
  # invalid credentials
rescue Sockudo::HTTPError => e
  # network or HTTP-level error
rescue Sockudo::Error => e
  # catch-all
end
```

## Logging

Assign any logger compatible with Ruby's standard `Logger` interface:

```ruby
# Rails
Sockudo.logger = Rails.logger

# Default: logs at INFO level to STDOUT
```

## Testing

```bash
bundle install
bundle exec rake spec
```

## Pusher SDK Compatibility

Sockudo implements the full Pusher HTTP API. If you prefer to use the official `pusher` gem or are migrating from Pusher, point it at your Sockudo instance:

```ruby
require 'pusher'

pusher = Pusher::Client.new(
  app_id: 'your-app-id',
  key:    'your-app-key',
  secret: 'your-app-secret',
  host:   '127.0.0.1',
  port:    6001
)
```

All standard Pusher SDK calls work against a self-hosted Sockudo server without modification.

## License

MIT
