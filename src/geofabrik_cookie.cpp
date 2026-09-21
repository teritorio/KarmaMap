#include "geofabrik_cookie.hpp"

#include <curl/curl.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

namespace geofabrik_cookie {

std::string default_cookie_path(const std::string& output_dir) {
    if (output_dir.empty()) return ".geofabrik.cookie";
    if (output_dir.back() == '/') return output_dir + ".geofabrik.cookie";
    return output_dir + "/.geofabrik.cookie";
}

bool requires_auth(const std::string& url) {
    return url.find(kInternalHost) != std::string::npos;
}

bool has_credentials() {
    const char* user = std::getenv("OSM_GEOFABRIK_USER");
    const char* password = std::getenv("OSM_GEOFABRIK_PASSWORD");
    return user != nullptr && user[0] != '\0' && password != nullptr &&
           password[0] != '\0';
}

std::string csrf_token(const std::string& html) {
    const std::string needle = "name=\"csrf-token\" content=\"";
    const size_t pos = html.find(needle);
    if (pos == std::string::npos) return std::string();
    const size_t value_start = pos + needle.size();
    const size_t value_end = html.find('"', value_start);
    if (value_end == std::string::npos) return std::string();
    return html.substr(value_start, value_end - value_start);
}

std::string json_string_field(const std::string& text, const std::string& key) {
    const std::string prefix = "\"" + key + "\"";
    size_t pos = text.find(prefix);
    if (pos == std::string::npos) return std::string();
    pos += prefix.size();
    while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\t')) ++pos;
    if (pos >= text.size() || text[pos] != ':') return std::string();
    ++pos;
    while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\t')) ++pos;
    if (pos >= text.size() || text[pos] != '"') return std::string();
    ++pos;
    std::string out;
    while (pos < text.size() && text[pos] != '"') {
        if (text[pos] == '\\' && pos + 1 < text.size()) {
            out += text[++pos];
        } else {
            out += text[pos];
        }
        ++pos;
    }
    return out;
}

namespace {

constexpr char kUserAgent[] = "oauth_cookie_client.py";

size_t append_body(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* body = static_cast<std::string*>(userdata);
    body->append(ptr, size * nmemb);
    return size * nmemb;
}

// Keeps the last "location:" response header, trimmed.
size_t capture_location(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* location = static_cast<std::string*>(userdata);
    const size_t n = size * nmemb;
    std::string line(ptr, n);
    if (!line.empty() && line.back() == '\n') line.pop_back();
    if (!line.empty() && line.back() == '\r') line.pop_back();
    if (line.size() >= 9 &&
        (line.compare(0, 9, "location:") == 0 ||
         line.compare(0, 9, "Location:") == 0)) {
        std::string value = line.substr(9);
        while (!value.empty() && value.front() == ' ') value.erase(0, 1);
        *location = value;
    }
    return size * nmemb;
}

std::string url_encode(const std::string& value) {
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    for (unsigned char c : value) {
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' ||
            c == '~') {
            out += static_cast<char>(c);
        } else {
            out += '%';
            out += hex[c >> 4];
            out += hex[c & 0xF];
        }
    }
    return out;
}

// One HTTP request with libcurl: body sink, Location capture, error buffer
// and sane timeouts. The curl handle outlives each individual request so a
// caller can replay the same session (cookie engine) across several calls.
class CurlRequest {
public:
    CurlRequest() {
        handle_ = curl_easy_init();
        if (!handle_) throw std::runtime_error("Failed to initialize curl");
        curl_easy_setopt(handle_, CURLOPT_WRITEFUNCTION, append_body);
        curl_easy_setopt(handle_, CURLOPT_WRITEDATA, &body_);
        curl_easy_setopt(handle_, CURLOPT_HEADERFUNCTION, capture_location);
        curl_easy_setopt(handle_, CURLOPT_HEADERDATA, &location_);
        curl_easy_setopt(handle_, CURLOPT_USERAGENT, kUserAgent);
        curl_easy_setopt(handle_, CURLOPT_CONNECTTIMEOUT, 30L);
        curl_easy_setopt(handle_, CURLOPT_TIMEOUT, 120L);
        curl_easy_setopt(handle_, CURLOPT_ERRORBUFFER, errbuf_);
    }

    ~CurlRequest() { curl_easy_cleanup(handle_); }

    CURL* handle() { return handle_; }

    // Starts an in-memory cookie engine for a multi-request session.
    void enable_memory_cookies() {
        curl_easy_setopt(handle_, CURLOPT_COOKIEFILE, "");
    }

    // Loads a Netscape cookie jar (CURLOPT_COOKIEFILE), sent on every
    // subsequent request of this handle.
    void load_cookie_file(const std::string& path) {
        curl_easy_setopt(handle_, CURLOPT_COOKIEFILE, path.c_str());
    }

    std::string& location() { return location_; }

    // Performs the request and returns the HTTP status code. Throws a
    // std::runtime_error with a descriptive message on transport errors.
    long run(const std::string& context) {
        body_.clear();
        location_.clear();
        errbuf_[0] = '\0';
        const CURLcode res = curl_easy_perform(handle_);
        if (res != CURLE_OK) {
            const std::string detail =
                errbuf_[0] ? errbuf_ : curl_easy_strerror(res);
            long status = 0;
            curl_easy_getinfo(handle_, CURLINFO_RESPONSE_CODE, &status);
            throw std::runtime_error(context + ": " + detail + " (HTTP " +
                                     std::to_string(status) + ")");
        }
        long status = 0;
        curl_easy_getinfo(handle_, CURLINFO_RESPONSE_CODE, &status);
        return status;
    }

    const std::string& body() const { return body_; }

private:
    CURL* handle_;
    std::string body_;
    std::string location_;
    char errbuf_[CURL_ERROR_SIZE] = {0};
};

std::string require_env(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        throw std::runtime_error(std::string("Missing environment variable ") +
                                 name + "; set the OSM account in .env");
    }
    return value;
}

std::string require_csrf(const std::string& html, const std::string& what) {
    const std::string token = csrf_token(html);
    if (token.empty()) {
        throw std::runtime_error(what + ": no csrf-token in the login page");
    }
    return token;
}

std::string require_json_field(const std::string& body, const std::string& key,
                               const std::string& what) {
    const std::string value = json_string_field(body, key);
    if (value.empty()) {
        throw std::runtime_error(what + ": missing \"" + key +
                                 "\" field in the response");
    }
    return value;
}

void expect_status(long actual, long expected, const std::string& context) {
    if (actual != expected) {
        throw std::runtime_error(context + " returned HTTP " +
                                 std::to_string(actual) + " but expected " +
                                 std::to_string(expected));
    }
}

void write_jar(const std::string& jar_path, const std::string& body) {
    const std::filesystem::path parent =
        std::filesystem::path(jar_path).parent_path();
    if (!parent.empty()) std::filesystem::create_directories(parent);
    std::ofstream out(jar_path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        throw std::runtime_error("Failed to write cookie jar to " + jar_path);
    }
    out << body;
}

// Port of Geofabrik's oauth_cookie_client.py OAuth2 flow:
//  1. ask the consumer (get_cookie) for an OSM authorization URL
//  2. log into OSM (session held in-memory by the curl engine)
//  3. authorize the consumer
//  4. fetch the resulting Netscape cookie jar from the consumer's redirect
// The browser-side credential exchange lets karmamap obtain the internal
// server's session cookie without shipping Geofabrik's Python client.
void obtain_cookie(const std::string& jar_path) {
    const std::string user = require_env("OSM_GEOFABRIK_USER");
    const std::string password = require_env("OSM_GEOFABRIK_PASSWORD");

    // 1. Request an authorization URL from the Geofabrik consumer.
    std::string authorization_url, state, redirect_uri, client_id;
    {
        CurlRequest req;
        const std::string url = std::string(kConsumerUrl) +
                                "?action=get_authorization_url";
        curl_easy_setopt(req.handle(), CURLOPT_URL, url.c_str());
        curl_easy_setopt(req.handle(), CURLOPT_POST, 1L);
        curl_easy_setopt(req.handle(), CURLOPT_POSTFIELDS, "");
        expect_status(req.run("POST " + url), 200, "POST " + url);
        authorization_url =
            require_json_field(req.body(), "authorization_url", "POST " + url);
        state = require_json_field(req.body(), "state", "POST " + url);
        redirect_uri =
            require_json_field(req.body(), "redirect_uri", "POST " + url);
        client_id = require_json_field(req.body(), "client_id", "POST " + url);
    }

    // 2-3. OSM login and OAuth authorize on one session.
    std::string location;
    {
        CurlRequest session;
        session.enable_memory_cookies();

        const std::string login_url = std::string(kOsmHost) + "/login?cookie_test=true";
        curl_easy_setopt(session.handle(), CURLOPT_URL, login_url.c_str());
        curl_easy_setopt(session.handle(), CURLOPT_HTTPGET, 1L);
        expect_status(session.run("GET " + login_url), 200, "GET " + login_url);
        const std::string token = require_csrf(session.body(), "GET " + login_url);

        std::string form = "username=" + url_encode(user) + "&password=" +
                           url_encode(password) + "&referer=" +
                           url_encode("/") + "&commit=" +
                           url_encode("Login") + "&authenticity_token=" +
                           url_encode(token);
        const std::string post_url = std::string(kOsmHost) + "/login";
        curl_easy_setopt(session.handle(), CURLOPT_URL, post_url.c_str());
        curl_easy_setopt(session.handle(), CURLOPT_POST, 1L);
        curl_easy_setopt(session.handle(), CURLOPT_POSTFIELDS, form.c_str());
        expect_status(session.run("POST " + post_url), 302, "POST " + post_url);

        curl_easy_setopt(session.handle(), CURLOPT_URL,
                         authorization_url.c_str());
        curl_easy_setopt(session.handle(), CURLOPT_HTTPGET, 1L);
        const long status =
            session.run("GET " + authorization_url);
        if (status == 302) {
            // Authorization already granted to the consumer.
        } else if (status == 200) {
            const std::string authorize_token =
                require_csrf(session.body(), "GET " + authorization_url);
            std::string authorize_form =
                "client_id=" + url_encode(client_id) + "&redirect_uri=" +
                url_encode(redirect_uri) + "&authenticity_token=" +
                url_encode(authorize_token) + "&state=" + url_encode(state) +
                "&response_type=code&scope=" + url_encode("read_prefs") +
                "&nonce=&code_challenge=&code_challenge_method=&commit=" +
                url_encode("Authorize");
            curl_easy_setopt(session.handle(), CURLOPT_URL,
                             authorization_url.c_str());
            curl_easy_setopt(session.handle(), CURLOPT_POST, 1L);
            curl_easy_setopt(session.handle(), CURLOPT_POSTFIELDS,
                             authorize_form.c_str());
            expect_status(session.run("POST " + authorization_url), 302,
                          "POST " + authorization_url);
        } else {
            throw std::runtime_error("GET " + authorization_url +
                                     " returned HTTP " +
                                     std::to_string(status) +
                                     " but expected 200 or 302");
        }
        if (session.location().empty()) {
            throw std::runtime_error("Authorization redirect did not provide "
                                     "a location header");
        }
        location = session.location();

        // 4. Log out of the OSM session.
        const std::string logout_url = std::string(kOsmHost) + "/logout";
        curl_easy_setopt(session.handle(), CURLOPT_URL, logout_url.c_str());
        curl_easy_setopt(session.handle(), CURLOPT_HTTPGET, 1L);
        const long logout_status = session.run("GET " + logout_url);
        if (logout_status != 200 && logout_status != 302) {
            throw std::runtime_error("GET " + logout_url + " returned HTTP " +
                                     std::to_string(logout_status));
        }
    }

    // 5. Fetch the final cookie jar from the consumer's redirect.
    CurlRequest req;
    const std::string final_url = location + "&format=netscape";
    curl_easy_setopt(req.handle(), CURLOPT_URL, final_url.c_str());
    curl_easy_setopt(req.handle(), CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(req.handle(), CURLOPT_FOLLOWLOCATION, 1L);
    expect_status(req.run("GET " + final_url), 200, "GET " + final_url);
    write_jar(jar_path, req.body());
}

}  // namespace

void ensure_valid_cookie(const std::string& jar_path) {
    const bool probe_ran = std::filesystem::exists(jar_path);
    if (probe_ran) {
        CurlRequest req;
        req.load_cookie_file(jar_path);
        curl_easy_setopt(req.handle(), CURLOPT_URL, kCookieStatusUrl);
        curl_easy_setopt(req.handle(), CURLOPT_HTTPGET, 1L);
        const long status = req.run("GET " + std::string(kCookieStatusUrl));
        if (status == 200) {
            std::cerr << "[auth] Geofabrik cookie ok\n";
            return;
        }
        std::cerr << "[auth] Geofabrik cookie expired\n";
    } else {
        std::cerr << "[auth] no Geofabrik cookie yet\n";
    }

    std::cerr << "[auth] getting new Geofabrik cookie from the OSM account\n";
    obtain_cookie(jar_path);
}

}  // namespace geofabrik_cookie