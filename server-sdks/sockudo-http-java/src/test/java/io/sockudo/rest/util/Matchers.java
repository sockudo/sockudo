package io.sockudo.rest.util;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Objects;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import com.google.gson.Gson;

public class Matchers {

    public static <T> Matcher<HttpPost> field(final String fieldName, final T expected) {
        return new TypeSafeDiagnosingMatcher<HttpPost>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("HTTP request with field [" + fieldName + "], value [" + expected + "] in JSON body");
            }

            @Override
            public boolean matchesSafely(HttpPost item, Description mismatchDescription) {
                try {
                    @SuppressWarnings("unchecked")
                    T actual = (T)new Gson().fromJson(retrieveBody(item), Map.class).get(fieldName);
                    mismatchDescription.appendText("value was [" + actual + "]");
                    return expected.equals(actual);
                }
                catch (Exception e) {
                    return false;
                }
            }
        };
    }
    public static Matcher<HttpRequestBase> path(final String expected) {
        return new TypeSafeDiagnosingMatcher<HttpRequestBase>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("HTTP request with path [" + expected + "]");
            }

            @Override
            public boolean matchesSafely(HttpRequestBase item, Description mismatchDescription) {
                try {
                    String actual = item.getURI().getPath();
                    mismatchDescription.appendText("value was [" + actual + "]");
                    return expected.equals(actual);
                }
                catch (Exception e) {
                    return false;
                }
            }
        };
    }

    public static Matcher<HttpPost> stringBody(final String expected) {
        return new TypeSafeDiagnosingMatcher<HttpPost>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("HTTP request with body [" + expected + "]");
            }

            @Override
            public boolean matchesSafely(HttpPost item, Description mismatchDescription) {
                try {
                    String actual = retrieveBody(item);
                    mismatchDescription.appendText("Expected body [" + expected + "], but received [" + actual + "]");
                    return expected.equals(actual);
                }
                catch (Exception e) {
                    mismatchDescription.appendText("Encountered exception [" + e + "] attempting match");
                    return false;
                }
            }
        };
    }


    public static Matcher<HttpRequestBase> requestHeader(final String headerName, final String expectedValue) {
        return new TypeSafeDiagnosingMatcher<HttpRequestBase>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("HTTP request with header [" + headerName + "], value [" + expectedValue + "]");
            }

            @Override
            public boolean matchesSafely(HttpRequestBase item, Description mismatchDescription) {
                org.apache.http.Header h = item.getFirstHeader(headerName);
                if (h == null) {
                    mismatchDescription.appendText("header [" + headerName + "] was not present");
                    return false;
                }
                String actual = h.getValue();
                mismatchDescription.appendText("value was [" + actual + "]");
                return expectedValue.equals(actual);
            }
        };
    }

    public static Matcher<HttpPost> header(final String headerName, final String expectedValue) {
        return new TypeSafeDiagnosingMatcher<HttpPost>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("HTTP request with header [" + headerName + "], value [" + expectedValue + "]");
            }

            @Override
            public boolean matchesSafely(HttpPost item, Description mismatchDescription) {
                org.apache.http.Header h = item.getFirstHeader(headerName);
                if (h == null) {
                    mismatchDescription.appendText("header [" + headerName + "] was not present");
                    return false;
                }
                String actual = h.getValue();
                mismatchDescription.appendText("value was [" + actual + "]");
                return expectedValue.equals(actual);
            }
        };
    }

    public static Matcher<HttpRequestBase> queryParam(final String name, final String expectedValue) {
        return new TypeSafeDiagnosingMatcher<HttpRequestBase>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("HTTP request with query param [" + name + "], value [" + expectedValue + "]");
            }

            @Override
            public boolean matchesSafely(HttpRequestBase item, Description mismatchDescription) {
                final String query = item.getURI().getQuery();
                if (query == null) {
                    mismatchDescription.appendText("query string was empty");
                    return false;
                }
                final String[] pairs = query.split("&");
                for (final String pair : pairs) {
                    final String[] keyValue = pair.split("=", 2);
                    if (keyValue.length == 2 && name.equals(keyValue[0])) {
                        mismatchDescription.appendText("value was [" + keyValue[1] + "]");
                        return expectedValue.equals(keyValue[1]);
                    }
                }
                mismatchDescription.appendText("param [" + name + "] was not present");
                return false;
            }
        };
    }

    public static Matcher<HttpPost> fieldAndHeader(final String fieldName, final Object expectedFieldValue, final String headerName, final String expectedHeaderValue) {
        return new TypeSafeDiagnosingMatcher<HttpPost>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("HTTP request with field [" + fieldName + "]=[" + expectedFieldValue + "] and header [" + headerName + "]=[" + expectedHeaderValue + "]");
            }

            @Override
            public boolean matchesSafely(HttpPost item, Description mismatchDescription) {
                try {
                    final Object actualField = new Gson().fromJson(retrieveBody(item), Map.class).get(fieldName);
                    if (!Objects.equals(expectedFieldValue, actualField)) {
                        mismatchDescription.appendText("field value was [" + actualField + "]");
                        return false;
                    }
                } catch (Exception e) {
                    mismatchDescription.appendText("failed to read body");
                    return false;
                }
                org.apache.http.Header h = item.getFirstHeader(headerName);
                if (h == null) {
                    mismatchDescription.appendText("header [" + headerName + "] was not present");
                    return false;
                }
                String actualHeader = h.getValue();
                if (!expectedHeaderValue.equals(actualHeader)) {
                    mismatchDescription.appendText("header value was [" + actualHeader + "]");
                    return false;
                }
                return true;
            }
        };
    }

    private static String retrieveBody(HttpPost e) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        e.getEntity().writeTo(baos);
        return new String(baos.toByteArray(), "UTF-8");
    }

}
