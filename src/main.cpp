#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "logstat/aggregator.hpp"
#include "logstat/parser.hpp"

using namespace std;

static const char* kVersion = "0.6";

static void print_usage(std::ostream& os) {
  os << "Usage:\n"
     << "  logstat ingest --file <path> [--top N] [--percentiles p1,p2,...] [--format text|json] [--out <path>]\n"
     << "  logstat --help\n"
     << "  logstat --version\n"
     << "\n"
     << "Options:\n"
     << "  --file <path>           Input CSV log file.\n"
     << "  --top N                 Number of endpoints to display (default 10).\n"
     << "  --percentiles list      Comma-separated percentiles (default 50,95,99).\n"
     << "  --format text|json      Output format (default text).\n"
     << "  --out <path>            Write output to file instead of stdout.\n"
     << "  --help                  Print this help.\n"
     << "  --version               Print version.\n"
     << "\n"
     << "Examples:\n"
     << "  logstat ingest --file data/sample.csv\n"
     << "  logstat ingest --file data/sample.csv --top 5\n"
     << "  logstat ingest --file data/sample.csv --percentiles 50,90,95,99\n"
     << "  logstat ingest --file data/sample.csv --format json --out report.json\n";
}

static bool parse_int(const std::string& s, int& out) {
  try {
    size_t idx = 0;
    int v = std::stoi(s, &idx, 10);
    if (idx != s.size()) return false;
    out = v;
    return true;
  } catch (...) {
    return false;
  }
}

static std::vector<int> parse_percentile_list(const std::string& s, bool* ok_out) {
  std::vector<int> out;
  *ok_out = false;

  std::stringstream ss(s);
  std::string token;
  while (std::getline(ss, token, ',')) {
    size_t start = 0;
    while (start < token.size() && std::isspace(static_cast<unsigned char>(token[start]))) ++start;
    size_t end = token.size();
    while (end > start && std::isspace(static_cast<unsigned char>(token[end - 1]))) --end;
    token = token.substr(start, end - start);

    if (token.empty()) continue;

    int p = 0;
    if (!parse_int(token, p)) return {};
    if (p < 0 || p > 100) return {};
    out.push_back(p);
  }

  if (out.empty()) return {};

  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());

  *ok_out = true;
  return out;
}

static void print_status_text(std::ostream& os, const logstat::StatusBuckets& s, const std::string& indent) {
  os << indent << "2xx: " << s.s2xx << "\n"
     << indent << "3xx: " << s.s3xx << "\n"
     << indent << "4xx: " << s.s4xx << "\n"
     << indent << "5xx: " << s.s5xx << "\n"
     << indent << "other: " << s.other << "\n";
}

static std::string json_escape(const std::string& s) {
  std::string out;
  out.reserve(s.size() + 8);
  for (char c : s) {
    switch (c) {
      case '\\': out += "\\\\"; break;
      case '"':  out += "\\\""; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          // skip control chars (minimal)
        } else {
          out += c;
        }
    }
  }
  return out;
}

static void write_json_report(std::ostream& os,
                              const logstat::Aggregator& agg,
                              int top_n,
                              const std::vector<int>& percentiles) {
  std::vector<std::pair<std::string, std::int64_t>> tops;
  tops.reserve(agg.endpoints().size());
  for (const auto& kv : agg.endpoints()) {
    tops.push_back({kv.first, kv.second.count});
  }
  std::sort(tops.begin(), tops.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; });

  const size_t limit = std::min<size_t>(static_cast<size_t>(top_n), tops.size());

  const auto& total_status = agg.total_status();
  const auto& total_lat = agg.total_latency();

  os << "{\n";
  os << "  \"version\": \"" << kVersion << "\",\n";
  os << "  \"total_requests\": " << agg.total_requests() << ",\n";

  os << "  \"status\": {\n";
  os << "    \"2xx\": " << total_status.s2xx << ",\n";
  os << "    \"3xx\": " << total_status.s3xx << ",\n";
  os << "    \"4xx\": " << total_status.s4xx << ",\n";
  os << "    \"5xx\": " << total_status.s5xx << ",\n";
  os << "    \"other\": " << total_status.other << "\n";
  os << "  },\n";

  os << "  \"latency_ms\": {\n";
  for (size_t i = 0; i < percentiles.size(); ++i) {
    int p = percentiles[i];
    os << "    \"p" << p << "\": " << total_lat.percentile(static_cast<double>(p));
    os << (i + 1 < percentiles.size() ? ",\n" : "\n");
  }
  os << "  },\n";

  os << "  \"top_endpoints\": [\n";
  for (size_t i = 0; i < limit; ++i) {
    const std::string& endpoint = tops[i].first;
    auto it = agg.endpoints().find(endpoint);
    if (it == agg.endpoints().end()) continue;
    const auto& stats = it->second;

    os << "    {\n";
    os << "      \"endpoint\": \"" << json_escape(endpoint) << "\",\n";
    os << "      \"count\": " << stats.count << ",\n";

    os << "      \"status\": {\n";
    os << "        \"2xx\": " << stats.status.s2xx << ",\n";
    os << "        \"3xx\": " << stats.status.s3xx << ",\n";
    os << "        \"4xx\": " << stats.status.s4xx << ",\n";
    os << "        \"5xx\": " << stats.status.s5xx << ",\n";
    os << "        \"other\": " << stats.status.other << "\n";
    os << "      },\n";

    os << "      \"latency_ms\": {\n";
    for (size_t j = 0; j < percentiles.size(); ++j) {
      int p = percentiles[j];
      os << "        \"p" << p << "\": "
         << stats.latency.percentile(static_cast<double>(p));
      os << (j + 1 < percentiles.size() ? ",\n" : "\n");
    }
    os << "      }\n";

    os << "    }" << (i + 1 < limit ? ",\n" : "\n");
  }
  os << "  ]\n";
  os << "}\n";
}

static void write_text_report(std::ostream& os,
                              const logstat::Aggregator& agg,
                              int top_n,
                              const std::vector<int>& percentiles) {
  os << "\n=== REPORT ===\n";
  os << "Total requests: " << agg.total_requests() << "\n";
  os << "Status:\n";
  print_status_text(os, agg.total_status(), "  ");

  const auto& h = agg.total_latency();
  os << "Latency (ms):\n";
  for (int p : percentiles) {
    os << "  p" << p << ": " << h.percentile(static_cast<double>(p)) << "\n";
  }

  std::vector<std::pair<std::string, std::int64_t>> tops;
  tops.reserve(agg.endpoints().size());
  for (const auto& kv : agg.endpoints()) {
    tops.push_back({kv.first, kv.second.count});
  }

  std::sort(tops.begin(), tops.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; });

  os << "\nTop endpoints:\n";
  const size_t limit = std::min<size_t>(static_cast<size_t>(top_n), tops.size());

  for (size_t i = 0; i < limit; ++i) {
    const std::string& endpoint = tops[i].first;

    auto it = agg.endpoints().find(endpoint);
    if (it == agg.endpoints().end()) continue;

    const auto& stats = it->second;

    os << "\n  " << endpoint << "\n";
    os << "    count: " << stats.count << "\n";

    os << "    status:\n";
    print_status_text(os, stats.status, "      ");

    os << "    latency (ms):\n";
    for (int p : percentiles) {
      os << "      p" << p << ": "
         << stats.latency.percentile(static_cast<double>(p)) << "\n";
    }
  }
}

int main(int argc, char** argv) {
  // Global flags
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--help" || a == "-h") {
      print_usage(std::cout);
      return 0;
    }
    if (a == "--version") {
      std::cout << "logstat v" << kVersion << "\n";
      return 0;
    }
  }

  // Must have a command
  if (argc < 2) {
    print_usage(std::cout);
    return 1;
  }

  std::string cmd = argv[1];
  if (cmd != "ingest") {
    print_usage(std::cout);
    return 1;
  }

  std::string file_path;
  int top_n = 10;
  std::vector<int> percentiles = {50, 95, 99};
  std::string format = "text";
  std::string out_path;

  for (int i = 2; i < argc; ++i) {
    std::string a = argv[i];

    if (a == "--file" && i + 1 < argc) {
      file_path = argv[++i];
      continue;
    }

    if (a == "--top" && i + 1 < argc) {
      int v = 0;
      if (!parse_int(argv[++i], v) || v <= 0) {
        std::cerr << "Invalid --top value (must be a positive integer).\n";
        return 1;
      }
      top_n = v;
      continue;
    }

    if (a == "--percentiles" && i + 1 < argc) {
      bool ok = false;
      percentiles = parse_percentile_list(argv[++i], &ok);
      if (!ok) {
        std::cerr << "Invalid --percentiles list. Example: --percentiles 50,90,95,99\n";
        return 1;
      }
      continue;
    }

    if (a == "--format" && i + 1 < argc) {
      format = argv[++i];
      if (format != "text" && format != "json") {
        std::cerr << "Invalid --format. Use: text or json\n";
        return 1;
      }
      continue;
    }

    if (a == "--out" && i + 1 < argc) {
      out_path = argv[++i];
      continue;
    }

    // ignore global flags here (already handled)
    if (a == "--help" || a == "-h" || a == "--version") {
      continue;
    }

    std::cerr << "Unknown argument: " << a << "\n";
    print_usage(std::cerr);
    return 1;
  }

  if (file_path.empty()) {
    std::cerr << "Missing --file <path>\n";
    return 1;
  }

  // Parse + aggregate
  logstat::Aggregator agg;
  std::string err;

  bool ok = logstat::parse_csv_file(
      file_path,
      [&](const logstat::Record& r) { agg.add(r); },
      &err);

  if (!ok) {
    std::cerr << "Error: " << err << "\n";
    return 1;
  }

  // Decide output stream
  std::ofstream fout;
  std::ostream* out = &std::cout;

  if (!out_path.empty()) {
    fout.open(out_path, std::ios::out | std::ios::trunc);
    if (!fout) {
      std::cerr << "Error: Failed to open output file: " << out_path << "\n";
      return 1;
    }
    out = &fout;
  }

  // Write report
  if (format == "json") {
    write_json_report(*out, agg, top_n, percentiles);
  } else {
    *out << "logstat v" << kVersion << "\n";
    write_text_report(*out, agg, top_n, percentiles);
  }

  return 0;
}
