<?php

namespace unit;

use PHPUnit\Framework\TestCase;
use Sockudo\Sockudo;

class ChannelInfoUnitTest extends TestCase
{
    /**
     * @var Sockudo
     */
    private $sockudo;

    protected function setUp(): void
    {
        $this->sockudo = new Sockudo('thisisaauthkey', 'thisisasecret', 1);
    }

    public function testTrailingColonChannelThrowsException(): void
    {
        $this->expectException(\Sockudo\SockudoException::class);

        $this->sockudo->get_channel_info('test_channel:');
    }

    public function testLeadingColonChannelThrowsException(): void
    {
        $this->expectException(\Sockudo\SockudoException::class);

        $this->sockudo->get_channel_info(':test_channel');
    }

    public function testLeadingColonNLChannelThrowsException(): void
    {
        $this->expectException(\Sockudo\SockudoException::class);

        $this->sockudo->get_channel_info(':\ntest_channel');
    }

    public function testTrailingColonNLChannelThrowsException(): void
    {
        $this->expectException(\Sockudo\SockudoException::class);

        $this->sockudo->get_channel_info('test_channel\n:');
    }
}
