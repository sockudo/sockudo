<?php

namespace unit;

use PHPUnit\Framework\TestCase;
use Sockudo\Sockudo;

class SockudoConstructorTest extends TestCase
{
    public function testUseTLSOptionWillSetHostAndPort(): void
    {
        $options = ['useTLS' => true];
        $sockudo = new Sockudo('app_key', 'app_secret', 'app_id', $options);

        $settings = $sockudo->getSettings();
        self::assertEquals('https', $settings['scheme'], 'https');
        self::assertEquals('localhost', $settings['host']);
        self::assertEquals('443', $settings['port']);
    }

    public function testUseTLSOptionWillBeOverwrittenByHostAndPortOptionsSetHostAndPort(): void
    {
        $options = [
            'useTLS' => true,
            'host' => 'test.com',
            'port' => '3000',
        ];
        $sockudo = new Sockudo('app_key', 'app_secret', 'app_id', $options);

        $settings = $sockudo->getSettings();
        self::assertEquals('http', $settings['scheme']);
        self::assertEquals($options['host'], $settings['host']);
        self::assertEquals($options['port'], $settings['port']);
    }

    public function testSchemeIsStrippedAndIgnoredFromHostInOptions(): void
    {
        $options = [
            'host' => 'http://test.com',
        ];
        $sockudo = new Sockudo('app_key', 'app_secret', 'app_id', $options);

        $settings = $sockudo->getSettings();
        self::assertEquals('https', $settings['scheme']);
        self::assertEquals('test.com', $settings['host']);
    }

    public function testClusterSetsANewHost(): void
    {
        $options = [
            'cluster' => 'eu',
        ];
        $sockudo = new Sockudo('app_key', 'app_secret', 'app_id', $options);

        $settings = $sockudo->getSettings();
        self::assertEquals('api-eu.sockudo.com', $settings['host']);
    }

    public function testClusterOptionIsOverriddenByHostIfItExists(): void
    {
        $options = [
            'cluster' => 'eu',
            'host' => 'api.staging.sockudo.com',
        ];
        $sockudo = new Sockudo('app_key', 'app_secret', 'app_id', $options);

        $settings = $sockudo->getSettings();
        self::assertEquals('api.staging.sockudo.com', $settings['host']);
    }

    public function testSetTimeoutOption(): void
    {
        $options = [
            'timeout' => 10,
        ];
        $sockudo = new Sockudo('app_key', 'app_secret', 'app_id', $options);

        $settings = $sockudo->getSettings();
        self::assertEquals(10, $settings['timeout']);
    }
}
