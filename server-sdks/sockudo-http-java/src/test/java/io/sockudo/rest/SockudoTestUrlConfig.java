package io.sockudo.rest;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.lang.reflect.Field;

import io.sockudo.rest.util.SockudoNoHttp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SockudoTestUrlConfig {

    @Test
    public void testUrl() throws Exception {
        SockudoNoHttp p = new SockudoNoHttp("https://key:secret@api.example.com:4433/apps/00001");

        assertField(p, "scheme", "https");
        assertField(p, "key", "key");
        assertField(p, "secret", "secret");
        assertField(p, "host", "api.example.com:4433");
        assertField(p, "appId", "00001");
    }

    @Test
    public void testUrlNoPort() throws Exception {
        SockudoNoHttp p = new SockudoNoHttp("http://key:secret@api.example.com/apps/00001");

        assertField(p, "scheme", "http");
        assertField(p, "key", "key");
        assertField(p, "secret", "secret");
        assertField(p, "host", "api.example.com");
        assertField(p, "appId", "00001");
    }

    @Test
    public void testUrlMissingField() throws Exception {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new SockudoNoHttp("https://key@api.example.com:4433/apps/appId");
        });
    }

    @Test
    public void testUrlEmptySecret() throws Exception {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new SockudoNoHttp("https://key:@api.example.com:4433/apps/appId");
        });
    }

    @Test
    public void testUrlEmptyKey() throws Exception {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new SockudoNoHttp("https://:secret@api.example.com:4433/apps/appId");
        });
    }

    @Test
    public void testUrlInvalidScheme() throws Exception {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new SockudoNoHttp("telnet://key:secret@api.example.com:4433/apps/appId");
        });
    }

    private static <T extends SockudoAbstract<?>, V> void assertField(final T o, final String fieldName, final V expected) throws Exception {
        final Field field = SockudoAbstract.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        final V actual = (V)field.get(o);

        assertThat(actual, is(expected));
    }
}
