<?php

namespace unit;

use GuzzleHttp\Client;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Sockudo\Sockudo;

class IdempotencyKeyTest extends TestCase
{
    public function testGenerateIdempotencyKeyReturnsValidUuidV4(): void
    {
        $key = Sockudo::generateIdempotencyKey();

        $this->assertMatchesRegularExpression('/^[A-Za-z0-9_-]{16}$/', $key);
    }

    public function testGenerateIdempotencyKeyReturnsUniqueValues(): void
    {
        $key1 = Sockudo::generateIdempotencyKey();
        $key2 = Sockudo::generateIdempotencyKey();

        $this->assertNotEquals($key1, $key2);
    }

    public function testTriggerWithStringIdempotencyKeySendsHeaderAndBody(): void
    {
        $container = [];
        $history = Middleware::history($container);

        $mock = new MockHandler([new Response(200, [], "{}")]);
        $handlerStack = HandlerStack::create($mock);
        $handlerStack->push($history);
        $client = new Client(["handler" => $handlerStack]);

        $sockudo = new Sockudo(
            "authkey",
            "secret",
            "1",
            ["host" => "localhost"],
            $client,
        );

        $sockudo->trigger(
            "test-channel",
            "test-event",
            ["hello" => "world"],
            [
                "idempotency_key" => "my-custom-key-123",
            ],
        );

        $this->assertCount(1, $container);
        $request = $container[0]["request"];

        $this->assertEquals(
            "my-custom-key-123",
            $request->getHeaderLine("X-Idempotency-Key"),
        );

        $body = json_decode((string) $request->getBody(), true);
        $this->assertEquals("my-custom-key-123", $body["idempotency_key"]);
    }

    public function testTriggerWithTrueIdempotencyKeyAutoGeneratesUuid(): void
    {
        $container = [];
        $history = Middleware::history($container);

        $mock = new MockHandler([new Response(200, [], "{}")]);
        $handlerStack = HandlerStack::create($mock);
        $handlerStack->push($history);
        $client = new Client(["handler" => $handlerStack]);

        $sockudo = new Sockudo(
            "authkey",
            "secret",
            "1",
            ["host" => "localhost"],
            $client,
        );

        $sockudo->trigger(
            "test-channel",
            "test-event",
            ["hello" => "world"],
            [
                "idempotency_key" => true,
            ],
        );

        $this->assertCount(1, $container);
        $request = $container[0]["request"];

        $headerValue = $request->getHeaderLine("X-Idempotency-Key");
        $this->assertMatchesRegularExpression(
            '/^[A-Za-z0-9_-]{16}$/',
            $headerValue,
        );

        $body = json_decode((string) $request->getBody(), true);
        $this->assertEquals($headerValue, $body["idempotency_key"]);
    }

    public function testTriggerWithoutIdempotencyKeyDoesNotSendHeader(): void
    {
        $container = [];
        $history = Middleware::history($container);

        $mock = new MockHandler([new Response(200, [], "{}")]);
        $handlerStack = HandlerStack::create($mock);
        $handlerStack->push($history);
        $client = new Client(["handler" => $handlerStack]);

        $sockudo = new Sockudo(
            "authkey",
            "secret",
            "1",
            [
                "host" => "localhost",
                "auto_idempotency_key" => false,
            ],
            $client,
        );

        $sockudo->trigger("test-channel", "test-event", ["hello" => "world"]);

        $this->assertCount(1, $container);
        $request = $container[0]["request"];

        $this->assertFalse($request->hasHeader("X-Idempotency-Key"));

        $body = json_decode((string) $request->getBody(), true);
        $this->assertArrayNotHasKey("idempotency_key", $body);
    }

    public function testTriggerBatchWithIdempotencyKeyInBody(): void
    {
        $container = [];
        $history = Middleware::history($container);

        $mock = new MockHandler([new Response(200, [], "{}")]);
        $handlerStack = HandlerStack::create($mock);
        $handlerStack->push($history);
        $client = new Client(["handler" => $handlerStack]);

        $sockudo = new Sockudo(
            "authkey",
            "secret",
            "1",
            [
                "host" => "localhost",
                "auto_idempotency_key" => false,
            ],
            $client,
        );

        $sockudo->triggerBatch([
            [
                "channel" => "channel-1",
                "name" => "event-1",
                "data" => ["msg" => "hello"],
                "idempotency_key" => "batch-key-1",
            ],
            [
                "channel" => "channel-2",
                "name" => "event-2",
                "data" => ["msg" => "world"],
            ],
            [
                "channel" => "channel-3",
                "name" => "event-3",
                "data" => ["msg" => "auto"],
                "idempotency_key" => true,
            ],
        ]);

        $this->assertCount(1, $container);
        $request = $container[0]["request"];

        $body = json_decode((string) $request->getBody(), true);
        $batch = $body["batch"];

        $this->assertEquals("batch-key-1", $batch[0]["idempotency_key"]);
        $this->assertArrayNotHasKey("idempotency_key", $batch[1]);
        $this->assertMatchesRegularExpression(
            '/^[A-Za-z0-9_-]{16}$/',
            $batch[2]["idempotency_key"],
        );
    }

    public function testTriggerAsyncWithIdempotencyKeySendsHeaderAndBody(): void
    {
        $container = [];
        $history = Middleware::history($container);

        $mock = new MockHandler([new Response(200, [], "{}")]);
        $handlerStack = HandlerStack::create($mock);
        $handlerStack->push($history);
        $client = new Client(["handler" => $handlerStack]);

        $sockudo = new Sockudo(
            "authkey",
            "secret",
            "1",
            ["host" => "localhost"],
            $client,
        );

        $promise = $sockudo->triggerAsync(
            "test-channel",
            "test-event",
            ["hello" => "world"],
            [
                "idempotency_key" => "async-key-456",
            ],
        );
        $promise->wait();

        $this->assertCount(1, $container);
        $request = $container[0]["request"];

        $this->assertEquals(
            "async-key-456",
            $request->getHeaderLine("X-Idempotency-Key"),
        );

        $body = json_decode((string) $request->getBody(), true);
        $this->assertEquals("async-key-456", $body["idempotency_key"]);
    }
}
