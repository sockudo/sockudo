<?php

namespace unit;

use PHPUnit\Framework\TestCase;
use Sockudo\Sockudo;
use Sockudo\SockudoException;

class WebhookTest extends TestCase
{
    /**
     * @var string
     */
    private $auth_key;
    /**
     * @var Sockudo
     */
    private $sockudo;

    protected function setUp(): void
    {
        $this->auth_key = 'thisisaauthkey';
        $this->sockudo = new Sockudo($this->auth_key, 'thisisasecret', 1);
    }

    private function loadForwardCompatFixture(string $name): string
    {
        $directory = __DIR__;
        while ($directory !== dirname($directory)) {
            $candidate = $directory . '/tests/ai-conformance/fixtures/forward-compat/' . $name;
            if (is_file($candidate)) {
                return file_get_contents($candidate);
            }
            $directory = dirname($directory);
        }

        self::fail('Forward compatibility fixture not found: ' . $name);
    }

    public function testValidWebhookSignature(): void
    {
        $signature = '40e0ad3b9aa49529322879e84de1aaaf18bde1efe839ca263d540cc865510d25';
        $body = '{"hello":"world"}';
        $headers = [
            'X-Pusher-Key'       => $this->auth_key,
            'X-Pusher-Signature' => $signature,
        ];

        $this->sockudo->ensure_valid_signature($headers, $body);

        self::assertTrue(true);
    }

    public function testInvalidWebhookSignature(): void
    {
        $this->expectException(SockudoException::class);

        $signature = 'potato';
        $body = '{"hello":"world"}';
        $headers = [
            'X-Pusher-Key'       => $this->auth_key,
            'X-Pusher-Signature' => $signature,
        ];
        $this->sockudo->ensure_valid_signature($headers, $body);
    }

    public function testDecodeWebhook(): void
    {
        $headers_json = '{"X-Pusher-Key":"' . $this->auth_key . '","X-Pusher-Signature":"a19cab2af3ca1029257570395e78d5d675e9e700ca676d18a375a7083178df1c"}';
        $body = '{"time_ms":1530710011901,"events":[{"name":"client_event","channel":"private-my-channel","event":"client-event","data":"Unencrypted","socket_id":"240621.35780774"}]}';
        $headers = json_decode($headers_json, true, 512, JSON_THROW_ON_ERROR);

        $decodedWebhook = $this->sockudo->webhook($headers, $body);
        self::assertEquals(1530710011901, $decodedWebhook->get_time_ms());
        self::assertCount(1, $decodedWebhook->get_events());
    }

    public function testForwardCompatWebhookFixture(): void
    {
        $body = $this->loadForwardCompatFixture('future-webhook-events.json');
        $headers = [
            'X-Pusher-Key'       => $this->auth_key,
            'X-Pusher-Signature' => hash_hmac('sha256', $body, 'thisisasecret'),
        ];

        $decodedWebhook = $this->sockudo->webhook($headers, $body);
        $events = $decodedWebhook->get_events();

        self::assertEquals(1710000000000, $decodedWebhook->get_time_ms());
        self::assertCount(3, $events);
        self::assertEquals('member_updated', $events[0]->name);
        self::assertEquals('must-pass-through', $events[0]->future_field);
        self::assertEquals('ai_run_started', $events[1]->name);
        self::assertEquals('run-1', $events[1]->run_id);
        self::assertEquals('message_version_created', $events[2]->name);
        self::assertEquals('ver-1', $events[2]->version_serial);
    }

    public function testNestedFutureWebhookValuesArePreserved(): void
    {
        $body = '{"time_ms":1710000000000,"events":[{"name":"ai_turn_started","channel":"private-ai-forward","data":{"turn_id":"turn-1","tokens":["hello","world"],"done":false,"nullable":null},"future_field":{"nested":true}}]}';
        $headers = [
            'X-Pusher-Key'       => $this->auth_key,
            'X-Pusher-Signature' => hash_hmac('sha256', $body, 'thisisasecret'),
        ];

        $decodedWebhook = $this->sockudo->webhook($headers, $body);
        $event = $decodedWebhook->get_events()[0];

        self::assertEquals('ai_turn_started', $event->name);
        self::assertEquals('turn-1', $event->data->turn_id);
        self::assertEquals(['hello', 'world'], $event->data->tokens);
        self::assertFalse($event->data->done);
        self::assertNull($event->data->nullable);
        self::assertTrue($event->future_field->nested);
    }
}
