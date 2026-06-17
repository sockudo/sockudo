<?php

namespace unit;

use GuzzleHttp;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Sockudo\Sockudo;
use Sockudo\SockudoException;

class PushHelpersUnitTest extends TestCase
{
    private $request_history = [];

    private function mockSockudo(array $responses): Sockudo
    {
        $mockHandler = new GuzzleHttp\Handler\MockHandler($responses);
        $history = GuzzleHttp\Middleware::history($this->request_history);
        $handlerStack = GuzzleHttp\HandlerStack::create($mockHandler);
        $handlerStack->push($history);
        $httpClient = new GuzzleHttp\Client(['handler' => $handlerStack]);

        return new Sockudo('auth-key', 'secret', 'appid', ['cluster' => 'test1'], $httpClient);
    }

    public function testListDeviceRegistrationsBuildsCursorQueryAndAdminHeader(): void
    {
        $sockudo = $this->mockSockudo([new Response(200, [], '{"items":[],"has_more":false,"next_cursor":null}')]);

        $result = $sockudo->listDeviceRegistrations([
            'limit' => 10,
            'cursor' => 'c1',
        ]);

        self::assertFalse($result->has_more);
        self::assertCount(1, $this->request_history);
        $request = $this->request_history[0]['request'];
        parse_str($request->getUri()->getQuery(), $query);

        self::assertEquals('GET', $request->getMethod());
        self::assertEquals('/apps/appid/push/deviceRegistrations', $request->getUri()->getPath());
        self::assertEquals('10', $query['limit']);
        self::assertEquals('c1', $query['cursor']);
        self::assertEquals('push-admin', $request->getHeaderLine('X-Sockudo-Push-Capability'));
    }

    public function testPublishPushDefaultsToAsyncAdminPublishAndAccepts202(): void
    {
        $sockudo = $this->mockSockudo([new Response(202, [], '{"publish_id":"pub_123","status":"queued"}')]);

        $result = $sockudo->publishPush([
            'recipients' => [['type' => 'channel', 'channel' => 'orders']],
            'payload' => ['title' => 'Order', 'body' => 'Updated'],
            'providerOverrides' => [['provider' => 'fcm', 'payload' => ['android' => new \stdClass()]]],
        ]);

        self::assertEquals('pub_123', $result->publish_id);
        self::assertCount(1, $this->request_history);
        $request = $this->request_history[0]['request'];
        $body = json_decode((string) $request->getBody(), true, 512, JSON_THROW_ON_ERROR);

        self::assertEquals('POST', $request->getMethod());
        self::assertEquals('/apps/appid/push/publish', $request->getUri()->getPath());
        self::assertFalse($body['sync']);
        self::assertEquals('fcm', $body['providerOverrides'][0]['provider']);
        self::assertEquals('push-admin', $request->getHeaderLine('X-Sockudo-Push-Capability'));
    }

    public function testListChannelPushSubscriptionsPassesDeviceIdentityToken(): void
    {
        $sockudo = $this->mockSockudo([new Response(200, [], '{"items":[],"has_more":false,"next_cursor":null}')]);

        $result = $sockudo->listChannelPushSubscriptions([
            'deviceId' => 'device-1',
            'limit' => 5,
        ], 'identity');

        self::assertFalse($result->has_more);
        self::assertCount(1, $this->request_history);
        $request = $this->request_history[0]['request'];
        parse_str($request->getUri()->getQuery(), $query);

        self::assertEquals('/apps/appid/push/channelSubscriptions', $request->getUri()->getPath());
        self::assertEquals('device-1', $query['deviceId']);
        self::assertEquals('5', $query['limit']);
        self::assertEquals('push-subscribe', $request->getHeaderLine('X-Sockudo-Push-Capability'));
        self::assertEquals('identity', $request->getHeaderLine('X-Sockudo-Device-Identity-Token'));
    }

    public function testSchedulePushRequiresNotBeforeMs(): void
    {
        $sockudo = $this->mockSockudo([]);

        $this->expectException(SockudoException::class);
        $this->expectExceptionMessage('scheduled push requires notBeforeMs');

        $sockudo->schedulePush([
            'recipients' => [['type' => 'channel', 'channel' => 'orders']],
            'payload' => ['title' => 'Order'],
        ]);
    }
}
