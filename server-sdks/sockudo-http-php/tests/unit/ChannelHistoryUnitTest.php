<?php

namespace unit;

use GuzzleHttp;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Sockudo\Sockudo;

class ChannelHistoryUnitTest extends TestCase
{
    private $request_history = [];

    private function mockSockudo(array $responses): Sockudo
    {
        $mockHandler = new GuzzleHttp\Handler\MockHandler($responses);
        $history = GuzzleHttp\Middleware::history($this->request_history);
        $handlerStack = GuzzleHttp\HandlerStack::create($mockHandler);
        $handlerStack->push($history);
        $httpClient = new GuzzleHttp\Client(['handler' => $handlerStack]);
        return new Sockudo("auth-key", "secret", "appid", ['cluster' => 'test1'], $httpClient);
    }

    public function testChannelHistoryBuildsExpectedRequestShape(): void
    {
        $sockudo = $this->mockSockudo([new Response(200, [], "{}")]);
        $sockudo->getChannelHistory('my-channel', [
            'limit' => 50,
            'direction' => 'newest_first',
            'cursor' => 'opaque-cursor',
            'start_serial' => 10,
            'end_serial' => 20,
            'start_time_ms' => 1000,
            'end_time_ms' => 2000,
        ]);

        self::assertEquals(1, count($this->request_history));
        $request = $this->request_history[0]['request'];
        parse_str($request->getUri()->getQuery(), $query);

        self::assertEquals('GET', $request->getMethod());
        self::assertEquals('/apps/appid/channels/my-channel/history', $request->getUri()->getPath());
        self::assertEquals('50', $query['limit']);
        self::assertEquals('newest_first', $query['direction']);
        self::assertEquals('opaque-cursor', $query['cursor']);
        self::assertEquals('10', $query['start_serial']);
        self::assertEquals('20', $query['end_serial']);
        self::assertEquals('1000', $query['start_time_ms']);
        self::assertEquals('2000', $query['end_time_ms']);
    }
}
