#pragma once

#include <cstdint>
#include <string>

namespace logstat {

// One parsed log line (CSV row)
struct Record {
  std::string timestamp;   // keep as string for MVP
  std::string service;
  std::string endpoint;
  std::int32_t status = 0;
  std::int32_t latency_ms = 0;
};

} // namespace logstat
