<?php

namespace unit;

use PHPUnit\Framework\TestCase;
use Sockudo\Sockudo;
use Sockudo\SockudoException;

class AuthorizeChannelTest extends TestCase
{
    /**
     * @var Sockudo
     */
    private $sockudo;

    protected function setUp(): void
    {
        $this->sockudo = new Sockudo("thisisaauthkey", "thisisasecret", 1, []);
    }

    public function testObjectConstruct(): void
    {
        $this->assertNotNull(
            $this->sockudo,
            "Created new \Sockudo\Sockudo object",
        );
    }

    public function testAuthorizeChannel(): void
    {
        $auth_string = $this->sockudo->authorizeChannel(
            "testing_sockudo-php",
            "1.1",
        );
        self::assertEquals(
            '{"auth":"thisisaauthkey:409ba7df1e4aa4433af9639cbb2849c21787a279d260d9f64f0cea8ed04ceae2"}',
            $auth_string,
            "Auth string key valid",
        );
    }

    public function testComplexAuthorizeChannel(): void
    {
        $auth_string = $this->sockudo->authorizeChannel(
            "-azAZ9_=@,.;",
            "45055.28877557",
        );
        self::assertEquals(
            '{"auth":"thisisaauthkey:d1c20ad7684c172271f92c108e11b45aef07499b005796ae1ec5beb924f361c4"}',
            $auth_string,
            "Auth string key valid",
        );
    }

    public function testAuthorizeChannelWithChannelData(): void
    {
        $auth_string = $this->sockudo->authorizeChannel(
            "-azAZ9_=@,.;",
            "45055.28877557",
            '{"user_id": "123"}',
        );
        self::assertEquals(
            '{"auth":"thisisaauthkey:3b3f1dcc4d7d2f95dd10ed05562397b3287b102d4cccfacbf30eed2f1ffa3d69","channel_data":"{\"user_id\": \"123\"}"}',
            $auth_string,
            "Auth string key valid",
        );
    }

    public function testTrailingColonSocketIDThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->authorizeChannel("testing_sockudo-php", "1.1:");
    }

    public function testLeadingColonSocketIDThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->authorizeChannel("testing_sockudo-php", ":1.1");
    }

    public function testLeadingColonNLSocketIDThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->authorizeChannel("testing_sockudo-php", ':\n1.1');
    }

    public function testTrailingColonNLSocketIDThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->authorizeChannel("testing_sockudo-php", '1.1\n:');
    }

    public function testTrailingColonChannelThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->authorizeChannel("test_channel:", "1.1");
    }

    public function testLeadingColonChannelThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->authorizeChannel(":test_channel", "1.1");
    }

    public function testLeadingColonNLChannelThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->authorizeChannel(':\ntest_channel', "1.1");
    }

    public function testTrailingColonNLChannelThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $this->sockudo->authorizeChannel('test_channel\n:', "1.1");
    }
}
