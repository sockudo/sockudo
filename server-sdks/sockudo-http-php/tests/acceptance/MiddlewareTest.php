<?php

namespace acceptance;

use Closure;
use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Handler\CurlHandler;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Sockudo\Sockudo;

class MiddlewareTest extends TestCase
{
    private $count = 0;
    /**
     * @var Sockudo
     */
    private $sockudo;

    public function increment(): Closure
    {
        return function (callable $handler) {
            return function (RequestInterface $request, array $options) use ($handler) {
                $this->count++;
                return $handler($request, $options);
            };
        };
    }

    protected function setUp(): void
    {
        if (SOCKUDOAPP_AUTHKEY === '' || SOCKUDOAPP_SECRET === '' || SOCKUDOAPP_APPID === '') {
            self::markTestSkipped('Please set the
            SOCKUDOAPP_AUTHKEY, SOCKUDOAPP_SECRET and
            SOCKUDOAPP_APPID keys.');
        } else {
            $stack = new HandlerStack();
            $stack->setHandler(new CurlHandler());
            $stack->push($this->increment());
            $client = new Client(['handler' => $stack]);
            $this->sockudo = new Sockudo(SOCKUDOAPP_AUTHKEY, SOCKUDOAPP_SECRET, SOCKUDOAPP_APPID, ['cluster' => SOCKUDOAPP_CLUSTER], $client);
        }
    }

    public function testStringPush(): void
    {
        self::assertEquals(0, $this->count);
        $result = $this->sockudo->trigger('test_channel', 'my_event', 'Test string');
        self::assertEquals(1, $this->count);
    }
}
