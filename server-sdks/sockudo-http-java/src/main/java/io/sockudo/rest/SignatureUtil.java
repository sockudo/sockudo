package io.sockudo.rest;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Hex;
import org.apache.http.client.utils.URIBuilder;

import io.sockudo.rest.util.Prerequisites;

public class SignatureUtil {

    public static URI uri(final String method,
                          final String scheme,
                          final String host,
                          final String path,
                          final String body,
                          final String key,
                          final String secret,
                          final Map<String, String> extraParams) {

        Prerequisites.noReservedKeys(extraParams);

        try {
            final Map<String, String> allParams = new HashMap<String, String>(extraParams);
            allParams.put("auth_key", key);
            allParams.put("auth_version", "1.0");
            allParams.put("auth_timestamp", Long.toString(System.currentTimeMillis() / 1000));
            if (body != null) {
                allParams.put("body_md5", bodyMd5(body));
            }

            // This is where the auth gets a bit weird. The query params for the request must include
            // the auth signature which is a signature over all the params except itself.
            allParams.put("auth_signature", sign(buildSignatureString(method, path, allParams), secret));

            final URIBuilder b = new URIBuilder()
                    .setScheme(scheme)
                    .setHost(host)
                    .setPath(path);

            for (final Entry<String, String> e : allParams.entrySet()) {
                b.setParameter(e.getKey(), e.getValue());
            }

            return b.build();
        }
        catch (final URISyntaxException e) {
            throw new RuntimeException("Could not build URI", e);
        }
    }

    private static String bodyMd5(final String body) {
        try {
            final MessageDigest md = MessageDigest.getInstance("MD5");
            final byte[] digest = md.digest(body.getBytes("UTF-8"));
            return Hex.encodeHexString(digest);
        }
        // If this doesn't exist, we're pretty much out of luck.
        catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("The Sockudo HTTP client requires MD5 support", e);
        }
        catch (final UnsupportedEncodingException e) {
            throw new RuntimeException("The Sockudo HTTP client needs UTF-8 support", e);
        }
    }

    public static String sign(final String input, final String secret) {
        try {
            final Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(), "SHA256"));

            final byte[] digest = mac.doFinal(input.getBytes("UTF-8"));
            return Hex.encodeHexString(digest);
        }
        catch (final InvalidKeyException e) {
            /// We validate this when the key is first provided, so we should never encounter it here.
            throw new RuntimeException("Invalid secret key", e);
        }
        // If either of these doesn't exist, we're pretty much out of luck.
        catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("The Sockudo HTTP client requires HmacSHA256 support", e);
        }
        catch (final UnsupportedEncodingException e) {
            throw new RuntimeException("The Sockudo HTTP client needs UTF-8 support", e);
        }
    }

    // Visible for testing
    static String buildSignatureString(final String method, final String path, final Map<String, String> queryParams) {
        final StringBuilder sb = new StringBuilder();
        sb.append(method)
            .append('\n')
            .append(path)
            .append('\n');

        final Map<String, String> canonicalParams = new HashMap<String, String>();
        for (final Entry<String, String> e : queryParams.entrySet()) {
            canonicalParams.put(e.getKey().toLowerCase(Locale.ROOT), e.getValue());
        }

        final String[] keys = canonicalParams.keySet().toArray(new String[0]);
        Arrays.sort(keys);

        boolean first = true;
        for (final String key : keys) {
            if (!first) sb.append('&');
            else first = false;

            sb.append(key)
                .append('=')
                .append(canonicalParams.get(key));
        }

        return sb.toString();
    }
}
