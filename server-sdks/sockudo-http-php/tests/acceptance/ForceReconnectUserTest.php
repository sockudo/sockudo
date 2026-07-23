<?php

namespace acceptance;

use GuzzleHttp;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Exception\RequestException;
use PHPUnit\Framework\TestCase;
use Sockudo\ApiErrorException;
use Sockudo\Sockudo;
use Sockudo\SockudoException;
use stdClass;

class ForceReconnectUserTest extends TestCase
{
    private $request_history = [];

    /**
     * @var Sockudo
     */
    private $sockudo;

    protected function setUp(): void
    {
        if (SOCKUDOAPP_AUTHKEY === '' || SOCKUDOAPP_SECRET === '' || SOCKUDOAPP_APPID === '') {
            self::markTestSkipped('Please set the
            SOCKUDOAPP_AUTHKEY, SOCKUDOAPP_SECRET and
            SOCKUDOAPP_APPID keys.');
        } else {
            $history = GuzzleHttp\Middleware::history($this->request_history);
            $handlerStack = GuzzleHttp\HandlerStack::create();
            $handlerStack->push($history);
            $httpClient = new GuzzleHttp\Client(['handler' => $handlerStack]);
            $this->sockudo = new Sockudo(SOCKUDOAPP_AUTHKEY, SOCKUDOAPP_SECRET, SOCKUDOAPP_APPID, ['cluster' => SOCKUDOAPP_CLUSTER], $httpClient);
        }
    }

    public function testForceReconnectUser(): void
    {
        $result = $this->sockudo->forceReconnectUser("123");
        self::assertEquals(new stdClass(), $result);
        self::assertEquals(1, count($this->request_history));
        $request = $this->request_history[0]['request'];
        self::assertEquals('api-' . SOCKUDOAPP_CLUSTER . '.sockudo.com', $request->GetUri()->GetHost());
        self::assertEquals('POST', $request->GetMethod());
        self::assertEquals('/apps/' . SOCKUDOAPP_APPID . '/users/123/force_reconnect', $request->GetUri()->GetPath());
    }

    public function testForceReconnectUserAsync(): void
    {
        $result = $this->sockudo->forceReconnectUserAsync("123")->wait();
        self::assertEquals(new stdClass(), $result);
        self::assertEquals(1, count($this->request_history));
        $request = $this->request_history[0]['request'];
        self::assertEquals('api-' . SOCKUDOAPP_CLUSTER . '.sockudo.com', $request->GetUri()->GetHost());
        self::assertEquals('POST', $request->GetMethod());
        self::assertEquals('/apps/' . SOCKUDOAPP_APPID . '/users/123/force_reconnect', $request->GetUri()->GetPath());
    }

    public function testBadUserId(): void
    {
        $this->expectException(SockudoException::class);
        $this->sockudo->forceReconnectUser("");
    }

    public function testBadUserIdAsync(): void
    {
        $this->expectException(SockudoException::class);
        $this->sockudo->forceReconnectUserAsync("");
    }
}
