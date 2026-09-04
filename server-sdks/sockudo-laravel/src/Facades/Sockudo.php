<?php

declare(strict_types=1);

namespace Sockudo\Laravel\Facades;

use Illuminate\Support\Facades\Facade;
use Sockudo\Laravel\SockudoManager;

/**
 * @method static \Sockudo\SockudoInterface connection(?string $name = null)
 * @method static void purge(?string $name = null)
 * @method static object trigger(array|string $channels, string $event, mixed $data, array $params = [], bool $already_encoded = false)
 * @method static object getChannelHistory(string $channel, array $params = [])
 * @method static object getPresenceHistory(string $channel, array $params = [])
 * @method static object getMessage(string $channel, string $messageSerial)
 * @method static object updateMessage(string $channel, string $messageSerial, array $params = [])
 * @method static object deleteMessage(string $channel, string $messageSerial, array $params = [])
 * @method static object appendMessage(string $channel, string $messageSerial, array $params = [])
 * @method static object publishAnnotation(string $channel, string $messageSerial, array $params)
 * @method static object publishPush(array $request)
 * @method static object createApnsLiveActivityChannel(string $storagePolicy = 'noStorage')
 * @method static object getApnsLiveActivityChannel(string $channelId)
 * @method static object listApnsLiveActivityChannels()
 * @method static object deleteApnsLiveActivityChannel(string $channelId)
 * @method static object getPublishStatus(string $publishId)
 *
 * @see SockudoManager
 */
class Sockudo extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return SockudoManager::class;
    }
}
