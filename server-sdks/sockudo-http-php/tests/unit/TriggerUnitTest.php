<?php

namespace unit;

use PHPUnit\Framework\TestCase;
use Sockudo\Sockudo;
use Sockudo\SockudoException;

class TriggerUnitTest extends TestCase
{
    /**
     * @var array
     */
    private $localData;
    /**
     * @var string
     */
    private $eventName;
    /**
     * @var Sockudo
     */
    private $sockudo;

    protected function setUp(): void
    {
        $this->sockudo = new Sockudo('thisisaauthkey', 'thisisasecret', 1);
        $this->eventName = 'test_event';
        $this->localData = [];
    }

    public function testTrailingColonChannelThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->trigger('test_channel:', $this->eventName, $this->localData);
    }

    public function testLeadingColonChannelThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->trigger(':test_channel', $this->eventName, $this->localData);
    }

    public function testLeadingColonNLChannelThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->trigger(':\ntest_channel', $this->eventName, $this->localData);
    }

    public function testTrailingColonNLChannelThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->trigger('test_channel\n:', $this->eventName, $this->localData);
    }

    public function testChannelArrayThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->trigger(['this_one_is_okay', 'test_channel\n:'], $this->eventName, $this->localData);
    }

    public function testTrailingColonSocketIDThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->trigger('test_channel:', $this->eventName, $this->localData, ['socket_id' => '1.1:']);
    }

    public function testLeadingColonSocketIDThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->trigger('test_channel:', $this->eventName, $this->localData, ['socket_id' => ':1.1']);
    }

    public function testLeadingColonNLSocketIDThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->trigger('test_channel:', $this->eventName, $this->localData, ['socket_id' => ':\n1.1']);
    }

    public function testTrailingColonNLSocketIDThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->trigger('test_channel:', $this->eventName, $this->localData, ['socket_id' => '1.1\n:']);
    }

    public function testFalseSocketIDThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->trigger('test_channel', $this->eventName, $this->localData, ['socket_id' => false]);
    }

    public function testEmptyStrSocketIDThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->trigger('test_channel', $this->eventName, $this->localData, ['socket_id' => '']);
    }
}
