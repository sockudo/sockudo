<?php

namespace acceptance;

use PHPUnit\Framework\TestCase;
use Sockudo\ApiErrorException;
use Sockudo\Sockudo;
use Sockudo\SockudoException;
use stdClass;

class TriggerTest extends TestCase
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

    public function testObjectConstruct(): void
    {
        self::assertNotNull($this->sockudo, 'Created new \Sockudo\Sockudo object');
    }

    public function testStringPush(): void
    {
        $result = $this->sockudo->trigger('test_channel', 'my_event', 'Test string');
        self::assertEquals(new stdClass(), $result);
    }

    public function testArrayPush(): void
    {
        $result = $this->sockudo->trigger('test_channel', 'my_event', ['test' => 1]);
        self::assertEquals(new stdClass(), $result);
    }

    public function testPushWithSocketId(): void
    {
        $result = $this->sockudo->trigger('test_channel', 'my_event', ['test' => 1], ['socket_id' => '123.456']);
        self::assertEquals(new stdClass(), $result);
    }

    public function testPushWithInfo(): void
    {
        $expectedMyChannel = new stdClass();
        $expectedMyChannel->subscription_count = 1;
        $expectedPresenceMyChannel = new stdClass();
        $expectedPresenceMyChannel->user_count = 0;
        $expectedPresenceMyChannel->subscription_count = 0;
        $expectedResult = new stdClass();
        $expectedResult->channels = [
            TEST_CHANNEL => $expectedMyChannel,
            "presence-my-channel" => $expectedPresenceMyChannel,
        ];

        $result = $this->sockudo->trigger([TEST_CHANNEL, 'presence-my-channel'], 'my_event', ['test' => 1], ['info' => 'user_count,subscription_count']);
        self::assertEquals($expectedResult, $result);
    }

    public function testTLSPush(): void
    {
        $options = [
            'useTLS' => true,
            'cluster' => SOCKUDOAPP_CLUSTER,
        ];
        $sockudo = new Sockudo(SOCKUDOAPP_AUTHKEY, SOCKUDOAPP_SECRET, SOCKUDOAPP_APPID, $options);

        $result = $sockudo->trigger('test_channel', 'my_event', ['encrypted' => 1]);
        self::assertEquals(new stdClass(), $result);
    }

    public function testSendingOver10kBMessageReturns413(): void
    {
        $this->expectException(ApiErrorException::class);
        $this->expectExceptionCode('413');

        $data = str_pad('', 11 * 1024, 'a');
        $this->sockudo->trigger('test_channel', 'my_event', $data, [], true);
    }

    public function testTriggeringEventOnOver100ChannelsThrowsException(): void
    {
        $this->expectException(SockudoException::class);

        $channels = [];
        while (count($channels) <= 101) {
            $channels[] = ('channel-' . count($channels));
        }
        $data = ['event_name' => 'event_data'];
        $this->sockudo->trigger($channels, 'my_event', $data);
    }

    public function testTriggeringEventOnMultipleChannels(): void
    {
        $data = ['event_name' => 'event_data'];
        $channels = ['test_channel_1', 'test_channel_2'];
        $result = $this->sockudo->trigger($channels, 'my_event', $data);
        self::assertEquals(new stdClass(), $result);
    }

    public function testTriggeringEventOnPrivateEncryptedChannelSuccess(): void
    {
        $options = ['encryption_master_key_base64' => 'Y0F6UkgzVzlGWk0zaVhxU05JR3RLenR3TnVDejl4TVY=',
            'cluster' => SOCKUDOAPP_CLUSTER];
        $this->sockudo = new Sockudo(SOCKUDOAPP_AUTHKEY, SOCKUDOAPP_SECRET, SOCKUDOAPP_APPID, $options);

        $data = ['event_name' => 'event_data'];
        $channels = ['private-encrypted-ceppaio'];
        $result = $this->sockudo->trigger($channels, 'my_event', $data);
        self::assertEquals(new stdClass(), $result);
    }

    public function testTriggeringEventOnMultipleChannelsWithEncryptedChannelPresentError(): void
    {
        $this->expectException(SockudoException::class);

        $options = ['encryption_master_key_base64' => 'Y0F6UkgzVzlGWk0zaVhxU05JR3RLenR3TnVDejl4TVY=',
            'cluster' => SOCKUDOAPP_CLUSTER];
        $this->sockudo = new Sockudo(SOCKUDOAPP_AUTHKEY, SOCKUDOAPP_SECRET, SOCKUDOAPP_APPID, $options);

        $data = ['event_name' => 'event_data'];
        $channels = ['my-chan-ceppaio', 'private-encrypted-ceppaio'];
        $this->sockudo->trigger($channels, 'my_event', $data);
    }

    public function testTriggeringApiExceptionIfConnectionErrorOcurred(): void
    {
        $this->expectException(ApiErrorException::class);

        $options = ['host' => 'invalidhost'];
        $this->sockudo = new Sockudo(SOCKUDOAPP_AUTHKEY, SOCKUDOAPP_SECRET, SOCKUDOAPP_APPID, $options);

        $this->sockudo->trigger('test_channel', 'my_event', 'event_data');
    }
}
