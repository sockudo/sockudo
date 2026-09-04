# Sockudo for Laravel

Official Laravel broadcasting driver and service-container integration for
[Sockudo](https://github.com/sockudo/sockudo).

The package keeps normal Laravel events on Sockudo's Pusher-compatible Protocol
V1 surface while exposing native history, mutable messages, annotations, and
push APIs through the Sockudo facade.

## Requirements

- PHP 8.2 or newer
- Laravel 12 or 13
- A reachable Sockudo server and configured application credentials

## Install

```bash
composer require sockudo/laravel
php artisan sockudo:install
```

Configure the application without committing secrets:

```dotenv
BROADCAST_CONNECTION=sockudo

SOCKUDO_APP_ID=app-id
SOCKUDO_APP_KEY=app-key
SOCKUDO_APP_SECRET=app-secret
SOCKUDO_HOST=127.0.0.1
SOCKUDO_PORT=6001
SOCKUDO_SCHEME=http
```

Use HTTPS whenever the HTTP API crosses an untrusted network. Validate the
configuration and signed HTTP API connection with:

```bash
php artisan sockudo:check
```

The package auto-discovers its service provider and adds a `sockudo`
broadcasting connection unless the application already defines one with that
name. Publish `config/sockudo.php` when you need to customize the connection.
If `routes/channels.php` is absent, install Laravel's broadcasting routes before
using private or presence channels; `sockudo:install` reports this condition.

## Broadcast Laravel events

Existing Laravel events work without Sockudo-specific interfaces:

```php
use Illuminate\Broadcasting\PrivateChannel;
use Illuminate\Contracts\Broadcasting\ShouldBroadcast;

final class OrderUpdated implements ShouldBroadcast
{
    public function __construct(public readonly string $orderId)
    {
    }

    public function broadcastOn(): PrivateChannel
    {
        return new PrivateChannel("orders.{$this->orderId}");
    }
}
```

`ShouldBroadcastNow`, queued broadcasts, `broadcast(...)->toOthers()`, private
channels, presence channels, user authentication, and encrypted private
channels use Laravel's normal broadcasting behavior.

Define private and presence authorization in `routes/channels.php` as usual:

```php
use Illuminate\Support\Facades\Broadcast;

Broadcast::channel('orders.{order}', function ($user, $order) {
    return $user->can('view', $order);
});
```

## Configure Laravel Echo

Protocol V1 remains compatible with Laravel Echo's Pusher connector:

```js
import Echo from 'laravel-echo';
import Pusher from 'pusher-js';

window.Pusher = Pusher;

window.Echo = new Echo({
    broadcaster: 'pusher',
    key: import.meta.env.VITE_SOCKUDO_APP_KEY,
    wsHost: import.meta.env.VITE_SOCKUDO_HOST,
    wsPort: Number(import.meta.env.VITE_SOCKUDO_PORT ?? 6001),
    wssPort: Number(import.meta.env.VITE_SOCKUDO_PORT ?? 443),
    forceTLS: (import.meta.env.VITE_SOCKUDO_SCHEME ?? 'https') === 'https',
    enabledTransports: ['ws', 'wss'],
});
```

Private and presence subscriptions still authorize through Laravel's
`/broadcasting/auth` route. Do not expose the app secret to JavaScript.

## Use native Sockudo APIs

The facade proxies to the default `sockudo` connection:

```php
use Sockudo\Laravel\Facades\Sockudo;

$page = Sockudo::getChannelHistory('orders', ['limit' => 50]);

Sockudo::updateMessage('orders', $messageSerial, [
    'data' => ['status' => 'paid'],
]);

Sockudo::publishAnnotation('orders', $messageSerial, [
    'type' => 'reaction',
    'name' => 'confirmed',
]);

$publish = Sockudo::publishPush([
    'recipients' => [['type' => 'channel', 'channel' => 'orders']],
    'payload' => ['title' => 'Order updated'],
    'publishId' => 'order-updated:ord-123:v4',
]);
```

Apple Live Activity broadcast channels are available through
`createApnsLiveActivityChannel`, `getApnsLiveActivityChannel`,
`listApnsLiveActivityChannels`, and `deleteApnsLiveActivityChannel`. Publish direct or broadcast
ActivityKit updates with `publishPush`; see the monorepo's
[Apple Live Activities guide](../../docs/content/docs/server/apple-live-activities.mdx).

Dependency injection is also available:

```php
use Sockudo\SockudoInterface;

final class LoadOrderHistory
{
    public function __construct(private SockudoInterface $sockudo)
    {
    }
}
```

For multiple apps, define additional `driver => sockudo` connections under
`broadcasting.connections`, then select one explicitly:

```php
$client = Sockudo::connection('sockudo-eu');
```

## Testing

```bash
composer install
vendor/bin/php-cs-fixer fix --dry-run --diff --using-cache=no
vendor/bin/phplint src config tests
vendor/bin/phpunit
```

Development happens in the Sockudo monorepo under
`server-sdks/sockudo-laravel`. Report issues in the main Sockudo repository.
