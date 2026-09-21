#pragma once

// Osmosis replication state ("state.txt") handling for --update-url.
//
// An OSM replication "update" base URL (e.g.
// https://osm-internal.download.geofabrik.de/africa/canary-islands-updates/)
// serves a state.txt in the osmosis format:
//
//   #comment line
//   sequenceNumber=2847632
//   timestamp=2019-12-30T09\:36\:32Z
//
// The sequence number and timestamp describe the replication state of a
// snapshot, which KarmaMap records alongside the update URL as provenance
// metadata in manifest.json. The update URL is stored (not the state.txt
// URL): it identifies the diff stream the snapshot came from.

#include <cstdint>
#include <string>

namespace replication_state {

struct State {
    uint64_t sequence_number;
    std::string timestamp;
    std::string url;  // the normalized update URL, trailing "/" guaranteed
};

// Returns the update URL with a trailing "/" appended if absent, so the
// stream identifier is unambiguous regardless of the user's spelling.
std::string normalize_update_url(const std::string& update_url);

// The state.txt URL derived from an update URL (<update_url>/state.txt).
std::string state_txt_url(const std::string& update_url);

// Parses osmosis-format state.txt text. Throws std::runtime_error when a
// field is missing or malformed.
State parse_state(const std::string& content, const std::string& url);

// Fetches <update_url>/state.txt over HTTP(S) and parses it. The returned
// State.url is the normalized update URL, not the state.txt URL. When
// cookie_file is non-empty, the Netscape cookie jar is sent with the request
// (CURLOPT_COOKIEFILE), as required by authenticated update streams such as
// osm-internal.download.geofabrik.de. Throws std::runtime_error on transport,
// HTTP or parse failures.
State fetch(const std::string& update_url, const std::string& cookie_file = "");

}  // namespace replication_state