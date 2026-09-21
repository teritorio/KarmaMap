#include "state.hpp"

#include <curl/curl.h>

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>

namespace replication_state {

std::string normalize_update_url(const std::string& update_url) {
    if (update_url.empty()) return update_url;
    if (update_url.back() == '/') return update_url;
    return update_url + "/";
}

std::string state_txt_url(const std::string& update_url) {
    return normalize_update_url(update_url) + "state.txt";
}

namespace {

// Extracts the value after "key=" on a non-comment, non-blank line.
std::string line_value(const std::string& line, const char* key) {
    const size_t key_len = std::string(key).size();
    if (line.size() <= key_len) return std::string();
    if (line.compare(0, key_len, key) != 0 || line[key_len] != '=') {
        return std::string();
    }
    return line.substr(key_len + 1);
}

bool parse_sequence(const std::string& value, uint64_t* out) {
    if (value.empty()) return false;
    uint64_t seq = 0;
    for (char c : value) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
        seq = seq * 10 + static_cast<uint64_t>(c - '0');
    }
    *out = seq;
    return true;
}

}  // namespace

State parse_state(const std::string& content, const std::string& url) {
    std::istringstream in(content);
    std::string line;
    bool found_seq = false, found_ts = false;
    State state;
    state.url = url;
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();  // CRLF
        if (line.empty() || line[0] == '#') continue;
        const std::string seq = line_value(line, "sequenceNumber");
        if (seq.size() > 0) {
            if (parse_sequence(seq, &state.sequence_number)) {
                found_seq = true;
            } else {
                throw std::runtime_error("State file " + url +
                                         " has a malformed sequenceNumber");
            }
            continue;
        }
        const std::string ts = line_value(line, "timestamp");
        if (ts.size() > 0) {
            state.timestamp = ts;
            found_ts = true;
        }
    }
    if (!found_seq) {
        throw std::runtime_error("State file " + url +
                                 " has no sequenceNumber field");
    }
    if (!found_ts) {
        throw std::runtime_error("State file " + url +
                                 " has no timestamp field");
    }
    return state;
}

namespace {

// curl write callback: appends the downloaded bytes to the std::string.
size_t append_body(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* body = static_cast<std::string*>(userdata);
    body->append(ptr, size * nmemb);
    return size * nmemb;
}

// Appends a hint when the failing request targets the authenticated
// internal Geofabrik server, explaining how the session cookie is obtained.
std::string auth_hint(const std::string& url) {
    if (url.find("osm-internal.download.geofabrik.de") == std::string::npos) {
        return std::string();
    }
    return " (the internal server requires a valid OSM session cookie; set "
           "OSM_GEOFABRIK_USER/OSM_GEOFABRIK_PASSWORD in .env so karmamap "
           "obtains one, or pass --cookie with an existing jar)";
}

}  // namespace

State fetch(const std::string& update_url, const std::string& cookie_file) {
    const std::string url = normalize_update_url(update_url);
    const std::string txt_url = url + "state.txt";

    CURL* curl = curl_easy_init();
    if (!curl) {
        throw std::runtime_error("Failed to initialize curl for " + txt_url);
    }

    std::string body;
    long status = 0;
    char errbuf[CURL_ERROR_SIZE] = {0};

    curl_easy_setopt(curl, CURLOPT_URL, txt_url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, append_body);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    if (!cookie_file.empty()) {
        curl_easy_setopt(curl, CURLOPT_COOKIEFILE, cookie_file.c_str());
    }

    const CURLcode res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        const std::string detail = errbuf[0] ? errbuf : curl_easy_strerror(res);
        throw std::runtime_error("Failed to fetch " + txt_url + ": " + detail +
                                 " (HTTP " + std::to_string(status) + ")" +
                                 auth_hint(url));
    }
    if (status != 200) {
        throw std::runtime_error("Failed to fetch " + txt_url + ": HTTP " +
                                 std::to_string(status) + auth_hint(url));
    }

    return parse_state(body, url);
}

}  // namespace replication_state