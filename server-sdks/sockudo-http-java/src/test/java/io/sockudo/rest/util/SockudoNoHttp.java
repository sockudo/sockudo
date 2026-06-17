package io.sockudo.rest.util;

import io.sockudo.rest.SockudoAbstract;

import java.net.URI;

public class SockudoNoHttp extends SockudoAbstract<Object> {

    public SockudoNoHttp(final String appId, final String key, final String secret) {
        super(appId, key, secret);
    }

    public SockudoNoHttp(final String url) {
        super(url);
    }

    @Override
    protected Object doGet(final URI uri) {
        throw new IllegalStateException("Shouldn't have been called, HTTP level not implemented");
    }

    @Override
    protected Object doDelete(final URI uri) {
        throw new IllegalStateException("Shouldn't have been called, HTTP level not implemented");
    }

    @Override
    protected Object doPost(final URI uri, final String body) {
        throw new IllegalStateException("Shouldn't have been called, HTTP level not implemented");
    }

}
