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

class TerminateUserConnectionsUnitTest extends TestCase
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

    public function testTerminateUserConections(): void
    {
        $sockudo = $this->mockSockudo([new Response(200, [], "{}")]);
        $result = $sockudo->terminateUserConnections("123");
        self::assertEquals(new stdClass(), $result);
        self::assertEquals(1, count($this->request_history));
        $request = $this->request_history[0]['request'];
        self::assertEquals('api-test1.sockudo.com', $request->GetUri()->GetHost());
        self::assertEquals('POST', $request->GetMethod());
        self::assertEquals('/apps/appid/users/123/terminate_connections', $request->GetUri()->GetPath());
    }

    public function testTerminateUserConectionsAsync(): void
    {
        $sockudo = $this->mockSockudo([new Response(200, [], "{}")]);
        $result = $sockudo->terminateUserConnectionsAsync("123")->wait();
        self::assertEquals(new stdClass(), $result);
        self::assertEquals(1, count($this->request_history));
        $request = $this->request_history[0]['request'];
        self::assertEquals('api-test1.sockudo.com', $request->GetUri()->GetHost());
        self::assertEquals('POST', $request->GetMethod());
        self::assertEquals('/apps/appid/users/123/terminate_connections', $request->GetUri()->GetPath());
    }

    public function testBadUserId(): void
    {
        $sockudo = $this->mockSockudo([]);
        $this->expectException(SockudoException::class);
        $sockudo->terminateUserConnections("");
    }

    public function testBadUserIdAsync(): void
    {
        $sockudo = $this->mockSockudo([]);
        $this->expectException(SockudoException::class);
        $sockudo->terminateUserConnectionsAsync("");
    }

    public function testTerminateUserConectionsError(): void
    {
        $sockudo = $this->mockSockudo([new Response(500, [], "{}")]);
        $this->expectException(ApiErrorException::class);
        $sockudo->terminateUserConnections("123");
    }

    public function testTerminateUserConectionsAsyncError(): void
    {
        $sockudo = $this->mockSockudo([new Response(500, [], "{}")]);
        $this->expectException(ApiErrorException::class);
        $sockudo->terminateUserConnectionsAsync("123")->wait();
    }
}
