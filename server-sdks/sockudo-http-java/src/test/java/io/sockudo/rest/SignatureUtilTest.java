package io.sockudo.rest;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class SignatureUtilTest {

    @Test
    public void stringToSignFields() {
        assertThat(SignatureUtil.buildSignatureString("POST", "/a/path", Collections.singletonMap("k", "v")),
                is("POST\n/a/path\nk=v"));

        assertThat(SignatureUtil.buildSignatureString("GET", "/a/nother/path", Collections.singletonMap("K", "V")),
                is("GET\n/a/nother/path\nk=V"));
    }

    @Test
    public void stringToSignQueryParamsOrderedByKey() {
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("a", "v1");
        params.put("zat", "v4");
        params.put("car", "v2");
        params.put("cat", "v3");

        // Make sure the iteration order is not incidentally alphabetical, or we're not testing what we intend to.
        String[] defaultOrder = params.keySet().toArray(new String[0]);
        String[] sortedOrder = params.keySet().toArray(new String[0]);
        Arrays.sort(sortedOrder);
        assertThat(defaultOrder, not(equalTo(sortedOrder)));

        String toSign = SignatureUtil.buildSignatureString("POST", "/", params);
        assertThat(toSign, containsString("a=v1&car=v2&cat=v3&zat=v4"));
    }

    @Test
    public void stringToSignLowercasesQueryKeysForSockudoVerification() {
        Map<String, String> params = new LinkedHashMap<String, String>();
        params.put("deviceId", "device-1");
        params.put("limit", "10");

        assertThat(SignatureUtil.buildSignatureString("GET", "/apps/app-id/push/channelSubscriptions", params),
                is("GET\n/apps/app-id/push/channelSubscriptions\ndeviceid=device-1&limit=10"));
    }

}
