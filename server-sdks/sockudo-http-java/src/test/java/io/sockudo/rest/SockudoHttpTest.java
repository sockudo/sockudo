package io.sockudo.rest;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.sockudo.rest.data.Result;
import io.sockudo.rest.data.Result.Status;

/**
 * Tests which use a local server to check response handling
 */
public class SockudoHttpTest {

    private HttpServer server;
    private HttpGet request;

    private int responseStatus = 200;
    private String responseBody;

    private Sockudo p;

    @BeforeEach
    public void setup() throws Exception {
        server = ServerBootstrap.bootstrap()
            .registerHandler("/*", (httpRequest, httpResponse, httpContext) -> {
                httpResponse.setStatusCode(responseStatus);
                if (responseBody != null) {
                    httpResponse.setEntity(new StringEntity(responseBody));
                }
            }).create();

        server.start();

        request = new HttpGet("http://" + server.getInetAddress().getHostName() + ":" + server.getLocalPort() + "/test");

        p = new Sockudo(SockudoTest.APP_ID, SockudoTest.KEY, SockudoTest.SECRET);
    }

    @AfterEach
    public void teardown() {
        server.stop();
    }

    @Test
    public void successReturnsOkAndBody() {
        responseStatus = 200;
        responseBody = "{}";

        Result result = p.httpCall(request);
        assertThat(result.getStatus(), is(Status.SUCCESS));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status201ReturnsOkAndBody() {
        responseStatus = 201;
        responseBody = "{\"deviceIdentityToken\":\"identity\"}";

        Result result = p.httpCall(request);
        assertThat(result.getStatus(), is(Status.SUCCESS));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status202ReturnsOkAndBody() {
        responseStatus = 202;
        responseBody = "{\"publish_id\":\"pub_123\",\"status\":\"queued\"}";

        Result result = p.httpCall(request);
        assertThat(result.getStatus(), is(Status.SUCCESS));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status400ReturnsGenericErrorAndMessage() {
        responseStatus = 400;
        responseBody = "A lolcat got all up in ur request";

        Result result = p.httpCall(request);
        assertThat(result.getStatus(), is(Status.CLIENT_ERROR));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status401ReturnsAuthenticationErrorAndMessage() {
        responseStatus = 401;
        responseBody = "Sorry, not in those shoes";

        Result result = p.httpCall(request);
        assertThat(result.getStatus(), is(Status.AUTHENTICATION_ERROR));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status403ReturnsAuthenticationErrorAndMessage() {
        responseStatus = 403;
        responseBody = "Sorry, not with all those friends";

        Result result = p.httpCall(request);
        assertThat(result.getStatus(), is(Status.MESSAGE_QUOTA_EXCEEDED));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status404ReturnsNotFoundErrorAndMessage() {
        responseStatus = 404;
        responseBody = "This is not the endpoint you are looking for";

        Result result = p.httpCall(request);
        assertThat(result.getStatus(), is(Status.NOT_FOUND));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status500ReturnsServerErrorAndMessage() {
        responseStatus = 500;
        responseBody = "Gary? Gary! It's on fire!!";

        Result result = p.httpCall(request);
        assertThat(result.getStatus(), is(Status.SERVER_ERROR));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status503ReturnsServerErrorAndMessage() {
        responseStatus = 503;
        responseBody = "Gary, did you restart all the back-ends at once?!";

        Result result = p.httpCall(request);
        assertThat(result.getStatus(), is(Status.SERVER_ERROR));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void connectionRefusedReturnsNetworkError() {
        server.stop(); // don't listen for this test

        Result result = p.httpCall(request);
        assertThat(result.getStatus(), is(Status.NETWORK_ERROR));
        assertThat(result.getMessage(), containsString("Connection refused"));
    }
}
