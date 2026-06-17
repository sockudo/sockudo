package io.sockudo.rest;

import io.sockudo.rest.data.Result;
import io.sockudo.rest.data.Result.Status;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.util.HttpConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Tests which use a local server to check response handling
 */
public class SockudoAsyncHttpTest {

    private HttpServer server;
    private Request request;

    private int responseStatus = 200;
    private String responseBody;

    private SockudoAsync p;

    @BeforeEach
    public void setup() throws Exception {
        server = ServerBootstrap.bootstrap()
            .registerHandler("/*", (httpRequest, httpResponse, httpContext) -> {
                httpResponse.setStatusCode(responseStatus);
                if (responseBody != null) {
                    httpResponse.setEntity(new StringEntity(responseBody));
                }
            })
            .create();

        server.start();

        request = new RequestBuilder(HttpConstants.Methods.GET)
                .setUrl("http://" + server.getInetAddress().getHostName() + ":" + server.getLocalPort() + "/test")
                .build();

        p = new SockudoAsync(SockudoTest.APP_ID, SockudoTest.KEY, SockudoTest.SECRET);
    }

    @AfterEach
    public void teardown() {
        server.stop();
    }

    @Test
    public void successReturnsOkAndBody() throws Exception {
        responseStatus = 200;
        responseBody = "{}";

        Result result = p.httpCall(request).get();
        assertThat(result.getStatus(), is(Status.SUCCESS));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status400ReturnsGenericErrorAndMessage() throws Exception {
        responseStatus = 400;
        responseBody = "A lolcat got all up in ur request";

        Result result = p.httpCall(request).get();
        assertThat(result.getStatus(), is(Status.CLIENT_ERROR));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status401ReturnsAuthenticationErrorAndMessage() throws Exception {
        responseStatus = 401;
        responseBody = "Sorry, not in those shoes";

        Result result = p.httpCall(request).get();
        assertThat(result.getStatus(), is(Status.AUTHENTICATION_ERROR));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status403ReturnsAuthenticationErrorAndMessage() throws Exception {
        responseStatus = 403;
        responseBody = "Sorry, not with all those friends";

        Result result = p.httpCall(request).get();
        assertThat(result.getStatus(), is(Status.MESSAGE_QUOTA_EXCEEDED));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status404ReturnsNotFoundErrorAndMessage() throws Exception {
        responseStatus = 404;
        responseBody = "This is not the endpoint you are looking for";

        Result result = p.httpCall(request).get();
        assertThat(result.getStatus(), is(Status.NOT_FOUND));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status500ReturnsServerErrorAndMessage() throws Exception {
        responseStatus = 500;
        responseBody = "Gary? Gary! It's still on fire!!";

        Result result = p.httpCall(request).get();
        assertThat(result.getStatus(), is(Status.SERVER_ERROR));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void status503ReturnsServerErrorAndMessage() throws Exception {
        responseStatus = 503;
        responseBody = "Gary, did you once again restart all the back-ends at once?!";

        Result result = p.httpCall(request).get();
        assertThat(result.getStatus(), is(Status.SERVER_ERROR));
        assertThat(result.getMessage(), is(responseBody));
    }

    @Test
    public void connectionRefusedReturnsUnknownError() throws Exception {
        server.stop(); // don't listen for this test

        Result result = p.httpCall(request).get();
        assertThat(result.getStatus(), is(Status.UNKNOWN_ERROR));
        assertThat(result.getMessage(), containsString("Connection refused"));
    }
}
