<?php

namespace unit;

use GuzzleHttp;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Exception\RequestException;
use PHPUnit\Framework\TestCase;
use Sockudo\ApiErrorException;
use Sockudo\Sockudo;
use Sockudo\SockudoException;
use stdClass;

class SendToUserTest extends TestCase
{
    private $request_history = [];

    private function mockSockudo(array $responses): Sockudo
    {
        $mockHandler = new GuzzleHttp\Handler\MockHandler($responses);
        $history = GuzzleHttp\Middleware::history($this->request_history);
        $handlerStack = GuzzleHttp\HandlerStack::create($mockHandler);
        $handlerStack->push($history);
        $httpClient = new GuzzleHttp\Client(["handler" => $handlerStack]);
        return new Sockudo(
            "auth-key",
            "secret",
            "appid",
            [
                "cluster" => "test1",
                "auto_idempotency_key" => false,
            ],
            $httpClient,
        );
    }

    public function testSendUser(): void
    {
        $sockudo = $this->mockSockudo([new Response(200, [], "{}")]);
        $result = $sockudo->sendToUser("123", "my-event", "event-data");
        self::assertEquals(new stdClass(), $result);
        self::assertEquals(1, count($this->request_history));
        $request = $this->request_history[0]["request"];
        self::assertEquals(
            "api-test1.sockudo.com",
            $request->GetUri()->GetHost(),
        );
        self::assertEquals("POST", $request->GetMethod());
        self::assertEquals("/apps/appid/events", $request->GetUri()->GetPath());
        self::assertEquals(
            '{"name":"my-event","data":"\"event-data\"","channel":"#server-to-user-123"}',
            (string) $request->GetBody(),
        );
    }

    public function testBadUserId(): void
    {
        $sockudo = $this->mockSockudo([]);
        $this->expectException(SockudoException::class);
        $sockudo->sendToUser("", "my-event", "event data");
    }

    public function testBadUserIdAsync(): void
    {
        $sockudo = $this->mockSockudo([]);
        $this->expectException(SockudoException::class);
        $sockudo->sendToUserAsync("", "my-event", "event data");
    }
}
