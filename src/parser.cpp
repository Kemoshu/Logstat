#include "logstat/parser.hpp"
#include <limits>
#include <cctype>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace logstat {

static void trim_inplace(std::string& s) {
  size_t start = 0;
  while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) ++start;
  size_t end = s.size();
  while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) --end;
  s = s.substr(start, end - start);
}

// CSV splitter with support for:
// - quoted fields: "a,b"
// - escaped quotes inside quoted fields: "" -> "
static bool split_csv_line(const std::string& line,
                           std::vector<std::string>& out,
                           std::string* err) {
  out.clear();

  std::string field;
  field.reserve(line.size());

  bool in_quotes = false;

  for (size_t i = 0; i < line.size(); ++i) {
    char c = line[i];

    if (in_quotes) {
      if (c == '"') {
        // Escaped quote inside quoted field: "" -> "
        if (i + 1 < line.size() && line[i + 1] == '"') {
          field.push_back('"');
          ++i;
        } else {
          in_quotes = false;
        }
      } else {
        field.push_back(c);
      }
      continue;
    }

    // Not in quotes
    if (c == '"') {
      in_quotes = true;
      continue;
    }

    if (c == ',') {
      trim_inplace(field);
      out.push_back(field);
      field.clear();
      continue;
    }

    field.push_back(c);
  }

  if (in_quotes) {
    if (err) *err = "Unterminated quoted field.";
    return false;
  }

  trim_inplace(field);
  out.push_back(field);
  return true;
}

static bool to_int32(const std::string& s, std::int32_t& out) {
  try {
    size_t idx = 0;
    long v = std::stol(s, &idx, 10);
    if (idx != s.size()) return false;
    if (v < std::numeric_limits<std::int32_t>::min() || v > std::numeric_limits<std::int32_t>::max())
      return false;
    out = static_cast<std::int32_t>(v);
    return true;
  } catch (...) {
    return false;
  }
}

static std::string short_line_preview(const std::string& line) {
  const size_t max_len = 160;
  if (line.size() <= max_len) return line;
  return line.substr(0, max_len) + "...";
}

bool parse_csv_file(const std::string& path,
                    const std::function<void(const Record&)>& on_record,
                    std::string* error_out) {
  std::ifstream in(path);
  if (!in) {
    if (error_out) *error_out = "Failed to open file: " + path;
    return false;
  }

  std::string line;
  std::vector<std::string> cols;

  // Read header
  int line_no = 0;
  while (std::getline(in, line)) {
    ++line_no;
    trim_inplace(line);
    if (!line.empty()) break; // skip leading blank lines
  }

  if (line.empty()) {
    if (error_out) *error_out = "Empty file (no header).";
    return false;
  }

  std::string err;
  if (!split_csv_line(line, cols, &err)) {
    if (error_out) {
      *error_out = "Header parse error at line " + std::to_string(line_no) + ": " + err +
                   " Line: " + short_line_preview(line);
    }
    return false;
  }

  if (cols.size() != 5 ||
      cols[0] != "timestamp" ||
      cols[1] != "service" ||
      cols[2] != "endpoint" ||
      cols[3] != "status" ||
      cols[4] != "latency_ms") {
    if (error_out) {
      *error_out = "Invalid header at line " + std::to_string(line_no) +
                   ". Expected: timestamp,service,endpoint,status,latency_ms"
                   ". Got: " + short_line_preview(line);
    }
    return false;
  }

  // Read rows
  while (std::getline(in, line)) {
    ++line_no;

    // allow blank lines
    std::string raw = line;
    trim_inplace(line);
    if (line.empty()) continue;

    if (!split_csv_line(raw, cols, &err)) {
      if (error_out) {
        *error_out = "CSV parse error at line " + std::to_string(line_no) + ": " + err +
                     " Line: " + short_line_preview(raw);
      }
      return false;
    }

    if (cols.size() != 5) {
      if (error_out) {
        *error_out = "Wrong column count at line " + std::to_string(line_no) +
                     " (expected 5, got " + std::to_string(cols.size()) + "). Line: " +
                     short_line_preview(raw);
      }
      return false;
    }

    Record r;
    r.timestamp = cols[0];
    r.service = cols[1];
    r.endpoint = cols[2];

    std::int32_t status = 0;
    std::int32_t latency = 0;

    if (!to_int32(cols[3], status)) {
      if (error_out) {
        *error_out = "Invalid status at line " + std::to_string(line_no) +
                     ". Value: " + cols[3] + ". Line: " + short_line_preview(raw);
      }
      return false;
    }

    if (!to_int32(cols[4], latency)) {
      if (error_out) {
        *error_out = "Invalid latency_ms at line " + std::to_string(line_no) +
                     ". Value: " + cols[4] + ". Line: " + short_line_preview(raw);
      }
      return false;
    }

    r.status = status;
    r.latency_ms = latency;

    on_record(r);
  }

  return true;
}

} // namespace logstat
