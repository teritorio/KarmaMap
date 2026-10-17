#include "suspect_atom.hpp"

#include "date_utils.hpp"
#include "suspect.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <cmath>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/table.h>

namespace suspect_atom {

namespace {

constexpr char kFeedTitle[] = "KarmaMap - suspects";
constexpr char kFeedId[] = "tag:karmamap,2026:suspects";
constexpr char kFeedUpdatedFallback[] = "1970-01-01T00:00:00Z";
constexpr char kOsmUserUrl[] = "https://www.openstreetmap.org/user/";
constexpr char kUsersPage[] = "../users/#user=";

std::string escape_xml(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        switch (c) {
            case '&': out += "&amp;"; break;
            case '<': out += "&lt;"; break;
            case '>': out += "&gt;"; break;
            case '"': out += "&quot;"; break;
            case '\'': out += "&#39;"; break;
            default: out += c;
        }
    }
    return out;
}

// Percent-encodes `s` for safe use in an href attribute value.
std::string url_encode(std::string_view s) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(s.size() * 2);
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            out += static_cast<char>(c);
        } else {
            out += '%';
            out += hex[c >> 4];
            out += hex[c & 0x0F];
        }
    }
    return out;
}

std::string utf8(char32_t cp) {
    std::string out;
    if (cp < 0x80) {
        out += static_cast<char>(cp);
    } else if (cp < 0x800) {
        out += static_cast<char>(0xC0 | (cp >> 6));
        out += static_cast<char>(0x80 | (cp & 0x3F));
    } else if (cp < 0x10000) {
        out += static_cast<char>(0xE0 | (cp >> 12));
        out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (cp & 0x3F));
    } else {
        out += static_cast<char>(0xF0 | (cp >> 18));
        out += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
        out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (cp & 0x3F));
    }
    return out;
}

// ISO alpha-2 code as a flag emoji plus the code (mirrors the web viewer).
std::string country_label(std::string_view code) {
    if (code.size() != 2) return std::string(code);
    char32_t cp[2];
    for (size_t i = 0; i < 2; ++i) {
        char upper = static_cast<char>(std::toupper(static_cast<unsigned char>(code[i])));
        cp[i] = 0x1F1E6 + (upper - 'A');
    }
    return utf8(cp[0]) + utf8(cp[1]) + " " + std::string(code);
}

// Formats a distance in meters as the web viewer does.
std::string format_distance(uint32_t meters) {
    if (meters >= 1000) {
        std::ostringstream out;
        out << (meters / 1000.0);
        std::string s = out.str();
        if (s.find('.') == std::string::npos) s += ".0";
        return s + " km";
    }
    std::ostringstream out;
    out << meters << " m";
    return out.str();
}

template <typename T>
const T* column_of(const arrow::Table& table, int idx) {
    return static_cast<const T*>(table.column(idx)->chunk(0).get());
}

// Reads one list<utf8> cell into a vector of strings.
std::vector<std::string> list_values(const arrow::Table& table, int idx, int64_t row) {
    const auto* list = column_of<arrow::ListArray>(table, idx);
    const auto& values = *list->values();
    const auto* strs = static_cast<const arrow::StringArray*>(&values);
    std::vector<std::string> out;
    int32_t start = list->value_offset(row);
    int32_t len = list->value_length(row);
    out.reserve(len);
    for (int32_t k = start; k < start + len; ++k)
        out.push_back(std::string(strs->GetView(k)));
    return out;
}

std::string filter_terms(uint8_t flag) {
    std::ostringstream out;
    const char* names[] = {"filter-2", "filter-3", "filter-1", "filter-4",
                           "filter-5", "name-language"};
    uint8_t bits[] = {suspect::kFlagFilter2, suspect::kFlagFilter3,
                      suspect::kFlagFilter1, suspect::kFlagFilter4,
                      suspect::kFlagFilter5, suspect::kFlagNameLanguage};
    bool first = true;
    for (size_t i = 0; i < std::size(names); ++i) {
        if (flag & bits[i]) {
            if (!first) out << ' ';
            out << "<category term='" << escape_xml(names[i]) << "'/>";
            first = false;
        }
    }
    return out.str();
}

}  // namespace

void write_feed(const arrow::Table& suspect_table, const std::string& atom_path) {
    const auto& schema = *suspect_table.schema();
    auto index = [&](const char* name) {
        int idx = schema.GetFieldIndex(name);
        if (idx < 0) {
            throw std::runtime_error("suspect table is missing the '" +
                                     std::string(name) + "' column");
        }
        return idx;
    };
    const int i_uid = index("uid");
    const int i_user = index("username");
    const int i_day = index("change_date");
    const int i_flag = index("suspect_flag");
    const int i_changes = index("changes");
    const int i_hour_edits = index("max_edits_per_hour");
    const int i_spread = index("peak_spread_km2");
    const int i_keys = index("changed_keys");
    const int i_countries = index("countries");
    const int i_moves = index("far_move_count");
    const int i_max_move = index("max_move_meters");
    const int i_rank = index("ranking_at_day");
    const int i_lang_events = index("name_lang_events");

    const auto* uids = column_of<arrow::Int64Array>(suspect_table, i_uid);
    const auto* users = column_of<arrow::StringArray>(suspect_table, i_user);
    const auto* days = column_of<arrow::UInt16Array>(suspect_table, i_day);
    const auto* flags = column_of<arrow::UInt8Array>(suspect_table, i_flag);
    const auto* changes = column_of<arrow::UInt32Array>(suspect_table, i_changes);
    const auto* hour_edits = column_of<arrow::UInt32Array>(suspect_table, i_hour_edits);
    const auto* spread = column_of<arrow::FloatArray>(suspect_table, i_spread);
    const auto* moves = column_of<arrow::UInt32Array>(suspect_table, i_moves);
    const auto* max_move = column_of<arrow::UInt32Array>(suspect_table, i_max_move);
    const auto* rank = column_of<arrow::UInt8Array>(suspect_table, i_rank);
    const auto* nle = column_of<arrow::UInt32Array>(suspect_table, i_lang_events);

    const int64_t n = suspect_table.num_rows();

    std::ostringstream feed;
    feed << "<?xml version='1.0' encoding='utf-8'?>\n";
    feed << "<feed xmlns='http://www.w3.org/2005/Atom'>\n";
    feed << "  <title>" << escape_xml(kFeedTitle) << "</title>\n";
    feed << "  <id>" << escape_xml(kFeedId) << "</id>\n";
    if (n > 0) {
        feed << "  <updated>" << date_utils::iso_date(days->Value(0))
             << "T00:00:00Z</updated>\n";
    } else {
        feed << "  <updated>" << kFeedUpdatedFallback << "</updated>\n";
    }
    feed << "  <link rel='self' href='/data/suspect.atom'/>\n";
    feed << "  <link rel='alternate' href='/suspect/'/>\n";

    for (int64_t i = 0; i < n; ++i) {
        const int64_t uid = uids->Value(i);
        const std::string user = std::string(users->GetView(i));
        const uint16_t day = days->Value(i);
        const std::string date = date_utils::iso_date(static_cast<int32_t>(day));
        const uint8_t flag = flags->Value(i);
        const uint32_t ch = changes->Value(i);
        const uint8_t r = rank->Value(i);

        const std::string entry_id =
            "tag:karmamap," + date + ":suspect/" + std::to_string(uid);
        const std::string escaped_user = escape_xml(user);

        feed << "  <entry>\n";
        feed << "    <title>" << escaped_user << " - " << date << " (" << ch
             << " changes)</title>\n";
        feed << "    <id>" << entry_id << "</id>\n";
        feed << "    <updated>" << date << "T00:00:00Z</updated>\n";
        feed << "    <author><name>" << escaped_user
             << "</name><uri>" << escape_xml(std::string(kOsmUserUrl) + url_encode(user))
             << "</uri></author>\n";
        feed << "    <link rel='alternate' href='" <<
                escape_xml(std::string(kUsersPage) + url_encode(user)) << "'/>\n";
        feed << filter_terms(flag);
        for (const std::string& code : list_values(suspect_table, i_countries, i)) {
            feed << "<category term='" << escape_xml(code) << "'/>";
        }
        feed << "\n";

        std::ostringstream content;
        content << "Flagged OSM edit day for " << escaped_user << " on " << date
                << ".\n";
        content << "Changes: " << ch << " | Ranking at day: #" << static_cast<int>(r)
                << "\n";
        const auto countries = list_values(suspect_table, i_countries, i);
        if (!countries.empty()) {
            std::ostringstream labels;
            for (size_t k = 0; k < countries.size(); ++k) {
                if (k) labels << ", ";
                labels << country_label(countries[k]);
            }
            content << "Countries: " << labels.str() << "\n";
        }
        if (flag & suspect::kFlagFilter2) {
            content << "Filter 2 (edit burst): peak "
                    << hour_edits->Value(i) << " edits/hour\n";
        }
        if (flag & suspect::kFlagFilter3) {
            content << "Filter 3 (node moves): " << moves->Value(i) << " far moves, "
                    << "largest " << format_distance(max_move->Value(i)) << "\n";
        }
        if (flag & suspect::kFlagFilter4) {
            content << "Filter 4 (spread): peak "
                    << static_cast<int>(std::lround(spread->Value(i)))
                    << " km^2\n";
        }
        if (flag & suspect::kFlagFilter5) {
            const auto keys = list_values(suspect_table, i_keys, i);
            content << "Filter 5 (mass tag):";
            for (const std::string& key : keys) {
                content << " " << escape_xml(key);
            }
            content << "\n";
        }
        if (flag & suspect::kFlagNameLanguage) {
            content << "Name-language abuse: "
                    << nle->Value(i) << " event(s)\n";
        }

        feed << "    <content type='text'>" << escape_xml(content.str())
             << "</content>\n";
        feed << "  </entry>\n";
    }

    feed << "</feed>\n";

    const std::string tmp = atom_path + ".tmp";
    {
        std::ofstream out(tmp);
        if (!out) {
            throw std::runtime_error("Failed to open " + tmp + " for writing");
        }
        out << feed.str();
        if (!out) {
            throw std::runtime_error("Failed writing " + tmp);
        }
    }
    std::filesystem::rename(tmp, atom_path);
}

}  // namespace suspect_atom
