#pragma once

// Session cookie handling for the Geofabrik internal download server
// (osm-internal.download.geofabrik.de), which serves user-attribute and
// full-history OSM extracts for OSM contributors only. Access requires a
// Netscape-format cookie jar that karmamap obtains itself from the OSM
// account in OSM_GEOFABRIK_USER / OSM_GEOFABRIK_PASSWORD, following the
// OAuth2 flow documented by Geofabrik's oauth_cookie_client.py.
//
// The jar is probed against <server>/cookie_status and refreshed when
// stale; the browser-side parses stay in C++ (state.cpp) which sends the
// jar via CURLOPT_COOKIEFILE.

#include <string>

namespace geofabrik_cookie {

// Host and endpoints of the internal Geofabrik server.
inline constexpr char kInternalHost[] = "osm-internal.download.geofabrik.de";
inline constexpr char kConsumerUrl[] =
    "https://osm-internal.download.geofabrik.de/get_cookie";
inline constexpr char kCookieStatusUrl[] =
    "https://osm-internal.download.geofabrik.de/cookie_status";
inline constexpr char kOsmHost[] = "https://www.openstreetmap.org";

// The default Netscape cookie jar path: <output-dir>/.geofabrik.cookie.
std::string default_cookie_path(const std::string& output_dir);

// True when the URL points at the internal Geofabrik server and therefore
// needs a session cookie.
bool requires_auth(const std::string& url);

// True when OSM_GEOFABRIK_USER and OSM_GEOFABRIK_PASSWORD are both set
// non-empty in the environment.
bool has_credentials();

// Refreshes the Netscape cookie jar at `jar_path` unless it already
// authenticates against the internal server. Throws std::runtime_error on
// transport failures and when the account is missing or the flow fails.
void ensure_valid_cookie(const std::string& jar_path);

// Parsing helpers, exposed for tests.
// Extracts the value of an HTML <meta name="csrf-token" content="..."> tag.
std::string csrf_token(const std::string& html);
// Extracts the string value of a JSON object field ("key": "value"),
// unescaping backslash escapes. Returns "" when absent.
std::string json_string_field(const std::string& text, const std::string& key);

}  // namespace geofabrik_cookie