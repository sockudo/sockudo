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

class SendToUserTest extends TestCase
{
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
            $this->sockudo = new Sockudo(SOCKUDOAPP_AUTHKEY, SOCKUDOAPP_SECRET, SOCKUDOAPP_APPID, ['cluster' => SOCKUDOAPP_CLUSTER]);
        }
    }

    public function testSendToUser(): void
    {
        $result = $this->sockudo->sendToUser('123', 'my_event', 'Test string');
        self::assertEquals(new stdClass(), $result);
    }

    public function testSendToUserAsync(): void
    {
        $result = $this->sockudo->sendToUserAsync('123', 'my_event', 'Test string')->wait();
        self::assertEquals(new stdClass(), $result);
    }

    public function testBadUserId(): void
    {
        $this->expectException(SockudoException::class);
        $this->sockudo->terminateUserConnections("");
    }

    public function testBadUserIdAsync(): void
    {
        $this->expectException(SockudoException::class);
        $this->sockudo->terminateUserConnectionsAsync("");
    }
}
