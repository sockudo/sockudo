# sockudo-php-server

Official PHP server SDK for [Sockudo](https://github.com/sockudo/sockudo) — a fast, self-hosted WebSocket server with full Pusher HTTP API compatibility.

## Supported Platforms

- **PHP** — 8.0, 8.1, 8.2, 8.3, 8.4
- **Laravel** — 8.29 and above (built-in Broadcasting driver support)
- Other PHP frameworks are supported provided you are running a supported PHP version.

## Installation

Clone the Sockudo monorepo and configure Composer to read the local package path until the package
is published on Packagist:

```bash
git clone https://github.com/sockudo/sockudo.git
composer config repositories.sockudo-php-server path ../sockudo/server-sdks/sockudo-http-php
composer require sockudo/sockudo-php-server:dev-main
```

Or add to `composer.json`:

```json
{
    "repositories": [
        {
            "type": "path",
            "url": "../sockudo/server-sdks/sockudo-http-php"
        }
    ],
    "require": {
        "sockudo/sockudo-php-server": "dev-main"
    }
}
```

Then run `composer update`.

## Constructor

```php
use Sockudo\Sockudo;

$sockudo = new Sockudo([
    'app_id'  => 'your-app-id',
    'key'     => 'your-app-key',
    'secret'  => 'your-app-secret',
    'host'    => '127.0.0.1',
    'port'    => 6001,
    'useTLS'  => false,
]);
```

### Options

| Option     | Type    | Default       | Description                        |
|------------|---------|---------------|------------------------------------|
| `app_id`   | string  | —             | Application ID                     |
| `key`      | string  | —             | Application key                    |
| `secret`   | string  | —             | Application secret                 |
| `host`     | string  | `127.0.0.1`  | Sockudo server host                |
| `port`     | int     | `6001`        | Sockudo server port                |
| `useTLS`   | bool    | `false`       | Use HTTPS                          |
| `scheme`   | string  | `http`        | URL scheme (`http` or `https`)     |
| `timeout`  | int     | `30`          | HTTP request timeout in seconds    |
| `path`     | string  | —             | Optional path prefix for all requests |

## Publishing Events

### Single Channel

```php
$sockudo->trigger('my-channel', 'my-event', ['message' => 'hello world']);
```

### Multiple Channels

```php
$sockudo->trigger(['channel-1', 'channel-2'], 'my-event', ['message' => 'hello world']);
```

### Excluding a Socket Recipient

Pass `socket_id` in the options array to prevent the sender from receiving its own event:

```php
$sockudo->trigger('my-channel', 'my-event', ['message' => 'hello'], ['socket_id' => $socket_id]);
```

### Batch Events

Send multiple events in a single HTTP request (up to 10 events per call):

```php
$batch = [
    ['channel' => 'channel-1', 'name' => 'event-1', 'data' => ['x' => 1]],
    ['channel' => 'channel-2', 'name' => 'event-2', 'data' => ['x' => 2]],
    ['channel' => 'channel-3', 'name' => 'event-3', 'data' => ['x' => 3]],
];

$sockudo->triggerBatch($batch);
```

### Asynchronous Publishing

Both `trigger` and `triggerBatch` have async counterparts that return Guzzle promises:

```php
$promise = $sockudo->triggerAsync(['channel-1', 'channel-2'], 'my-event', ['message' => 'hello']);

$promise->then(function ($result) {
    // handle result
});

$final = $promise->wait();
```

## Idempotent Publishing

Pass an `idempotency_key` to safely retry publishes without causing duplicate deliveries:

```php
$sockudo->trigger('my-channel', 'my-event', ['message' => 'hello'], [
    'idempotency_key' => 'order-shipped-order-789',
]);
```

The server deduplicates publishes with the same key within the configured window.

## Channel Authorization

### Private Channel

```php
$auth = $sockudo->authorizeChannel('private-my-channel', $socket_id);
echo $auth; // JSON: {"auth":"key:signature"}
```

### Presence Channel

```php
$auth = $sockudo->authorizePresenceChannel(
    'presence-my-channel',
    $socket_id,
    $user_id,
    ['name' => 'Jane Doe', 'role' => 'admin']
);
echo $auth; // JSON: {"auth":"key:signature","channel_data":"..."}
```

## User Authentication

```php
$auth = $sockudo->authenticateUser($socket_id, $user_id);
echo $auth;
```

## Webhooks

Pass the raw request headers and body to verify and parse incoming webhooks:

```php
$webhook = $sockudo->webhook($request_headers, $request_body);

$events = $webhook->get_events();
$time_ms = $webhook->get_time_ms();

foreach ($events as $event) {
    echo $event->name . ' on ' . $event->channel . PHP_EOL;
}
```

An exception is thrown if the signature cannot be validated.

## Application State Queries

```php
// Get info about a specific channel
$info = $sockudo->getChannelInfo('my-channel');
$occupied = $info->occupied;

// Get user count for a presence channel
$info = $sockudo->getChannelInfo('presence-my-channel', ['info' => 'user_count']);
$user_count = $info->user_count;

// List all channels
$result = $sockudo->getChannels();

// Filter channels by prefix
$result = $sockudo->getChannels(['filter_by_prefix' => 'presence-']);

// Get users in a presence channel
$result = $sockudo->getPresenceUsers('presence-my-channel');
```

## Laravel Integration

Laravel 8.29+ has built-in support for the Pusher Channels HTTP API as a [Broadcasting backend](https://laravel.com/docs/master/broadcasting). Since Sockudo is fully API-compatible, configure it in `config/broadcasting.php`:

```php
'pusher' => [
    'driver' => 'pusher',
    'key'    => env('SOCKUDO_APP_KEY'),
    'secret' => env('SOCKUDO_APP_SECRET'),
    'app_id' => env('SOCKUDO_APP_ID'),
    'options' => [
        'host'   => env('SOCKUDO_HOST', '127.0.0.1'),
        'port'   => env('SOCKUDO_PORT', 6001),
        'scheme' => 'http',
        'useTLS' => false,
    ],
],
```

## PSR-3 Logger Support

The `Sockudo` class implements `Psr\Log\LoggerAwareInterface`. Assign any PSR-3 compatible logger:

```php
// e.g. Monolog, Laravel's Log facade, etc.
$sockudo->setLogger($logger);
```

## Custom Guzzle Client

Pass your own Guzzle instance to customise HTTP behaviour (middleware, retry logic, etc.):

```php
$guzzle = new GuzzleHttp\Client([
    'timeout' => 5.0,
]);

$sockudo = new Sockudo(
    ['app_id' => '...', 'key' => '...', 'secret' => '...'],
    $guzzle
);
```

## Running the Tests

```bash
composer install
composer exec phpunit
```

## Channel History

```php
$page = $sockudo->getChannelHistory('my-channel', [
    'limit' => 50,
    'direction' => 'newest_first',
]);

$nextPage = $sockudo->getChannelHistory('my-channel', [
    'cursor' => 'opaque-cursor-from-previous-page',
]);
```

## Pusher SDK Compatibility

Sockudo implements the full Pusher HTTP API. If you prefer to use the official `pusher/pusher-php-server` package or are migrating from Pusher, point it at your Sockudo instance:

```php
$pusher = new Pusher\Pusher($key, $secret, $app_id, [
    'host'   => '127.0.0.1',
    'port'   => 6001,
    'scheme' => 'http',
]);
```

All standard Pusher SDK calls work against a self-hosted Sockudo server without modification.

## License

MIT
