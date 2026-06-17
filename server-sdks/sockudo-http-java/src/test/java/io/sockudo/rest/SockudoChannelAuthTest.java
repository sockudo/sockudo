package io.sockudo.rest;

import io.sockudo.rest.data.PresenceUser;
import io.sockudo.rest.util.SockudoNoHttp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SockudoChannelAuthTest {

    private final SockudoNoHttp p = new SockudoNoHttp("00001", "278d425bdf160c739803", "7ad3773142a6692b25b8");

    @Test
    public void privateChannelAuth() {
        assertThat(p.authenticate("1234.1234", "private-foobar"),
                is("{\"auth\":\"278d425bdf160c739803:58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4\"}"));
    }

    @Test
    public void complexPrivateChannelAuth() {
        assertThat(p.authenticate("1234.1234", "private-azAZ9_=@,.;"),
                is("{\"auth\":\"278d425bdf160c739803:208cbbce2a22fd7d7c3509046b17a97b99d345cf4c195bc0d54af9004a022b0b\"}"));
    }

    @Test
    public void privateChannelWrongPrefix() {

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate("1234.1234", "presence-foobar");
        });
    }

    @Test
    public void privateChannelNoPrefix() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate("1234.1234", "foobar");
        });
    }

    @Test
    public void trailingColonSocketId() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate("1.1:", "private-foobar");
        });
    }

    @Test
    public void trailingNLSocketId() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate("1.1\n", "private-foobar");
        });
    }

    @Test
    public void leadingColonSocketId() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate(":1.1", "private-foobar");
        });
    }

    @Test
    public void leadingColonNLSocketId() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate(":\n1.1", "private-foobar");
        });
    }

    @Test
    public void trailingColonNLSocketId() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate("1.1\n:", "private-foobar");
        });
    }

    @Test
    public void trailingColonChannel() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate("1.1", "private-foobar:");
        });
    }

    @Test
    public void leadingColonChannel() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate("1.1", ":private-foobar");
        });
    }

    @Test
    public void leadingColonNLChannel() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate("1.1", ":\nprivate-foobar");
        });
    }

    @Test
    public void trailingColonNLChannel() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate("1.1", "private-foobar\n:");
        });
    }

    @Test
    public void trailingNLChannel() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate("1.1", "private-foobar\n");
        });
    }

    @Test
    public void presenceChannelAuth() {
        assertThat(p.authenticate("1234.1234", "presence-foobar", new PresenceUser(Integer.valueOf(10), Collections.singletonMap("name", "Mr. Sockudo"))),
                is("{\"auth\":\"278d425bdf160c739803:2ac29a1d016a99a3605ea3b903450c61b8f9b301d9f7fd7da65d7ec7c17d69f9\",\"channel_data\":\"{\\\"user_id\\\":10,\\\"user_info\\\":{\\\"name\\\":\\\"Mr. Sockudo\\\"}}\"}"));
    }

    @Test
    public void presenceChannelWrongPrefix() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate("1234.1234", "private-foobar", new PresenceUser("dave"));
        });
    }

    @Test
    public void presenceChannelNoPrefix() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            p.authenticate("1234.1234", "foobar", new PresenceUser("dave"));
        });
    }

    @Test
    public void encryptedChannelWithoutMasterKey() {
        final Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            p.authenticate("1234.1234", "private-encrypted-foobar");
        });

        assertThat(
            exception.getMessage(),
            is(SockudoException.encryptionMasterKeyRequired().getMessage())
        );
    }
}
