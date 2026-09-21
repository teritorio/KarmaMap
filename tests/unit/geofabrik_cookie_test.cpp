#include <gtest/gtest.h>

#include <string>

#include "geofabrik_cookie.hpp"

namespace {

using geofabrik_cookie::csrf_token;
using geofabrik_cookie::default_cookie_path;
using geofabrik_cookie::json_string_field;
using geofabrik_cookie::requires_auth;

TEST(GeofabrikCookie, DefaultCookiePath) {
    EXPECT_EQ(default_cookie_path("out"), "out/.geofabrik.cookie");
    EXPECT_EQ(default_cookie_path("out/"), "out/.geofabrik.cookie");
    EXPECT_EQ(default_cookie_path(""), ".geofabrik.cookie");
}

TEST(GeofabrikCookie, RequiresAuthHost) {
    EXPECT_TRUE(requires_auth(
        "https://osm-internal.download.geofabrik.de/africa/region-updates/"));
    EXPECT_TRUE(requires_auth("osm-internal.download.geofabrik.de"));
    EXPECT_FALSE(requires_auth("https://download.geofabrik.de/africa/"));
    EXPECT_FALSE(requires_auth(""));
}

TEST(GeofabrikCookie, CsrfTokenExtraction) {
    const std::string html =
        "<head>"
        "<meta name=\"csrf-token\" content=\"ABCdef123+/=\" />"
        "</head>";
    EXPECT_EQ(csrf_token(html), "ABCdef123+/=");
}

TEST(GeofabrikCookie, CsrfTokenMissing) {
    EXPECT_EQ(csrf_token("<html><head></head></html>"), "");
    EXPECT_EQ(csrf_token(""), "");
}

TEST(GeofabrikCookie, JsonStringField) {
    const std::string body =
        "{\"authorization_url\": \"https://www.openstreetmap.org/oauth/"
        "authorize?client_id=x\", \"state\": \"s1\", \"redirect_uri\": "
        "\"https://osm-internal/download/cb\", \"client_id\": \"x\"}";
    EXPECT_EQ(json_string_field(body, "authorization_url"),
              "https://www.openstreetmap.org/oauth/authorize?client_id=x");
    EXPECT_EQ(json_string_field(body, "state"), "s1");
    EXPECT_EQ(json_string_field(body, "redirect_uri"),
              "https://osm-internal/download/cb");
    EXPECT_EQ(json_string_field(body, "client_id"), "x");
}

TEST(GeofabrikCookie, JsonStringFieldWithEscapes) {
    const std::string body = "{\"url\": \"a\\\"b\\\\c\"}";
    EXPECT_EQ(json_string_field(body, "url"), "a\"b\\c");
}

TEST(GeofabrikCookie, JsonStringFieldMissing) {
    EXPECT_EQ(json_string_field("{\"a\": 1}", "b"), "");
    EXPECT_EQ(json_string_field("", "a"), "");
    EXPECT_EQ(json_string_field("{\"a\": [1,2]}", "a"), "");
}

}  // namespace