#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "logstat/types.hpp"

namespace logstat {

struct StatusBuckets {
  std::int64_t s2xx = 0;
  std::int64_t s3xx = 0;
  std::int64_t s4xx = 0;
  std::int64_t s5xx = 0;
  std::int64_t other = 0;

  void add(std::int32_t status);
};

class LatencyHistogram {
public:
  // Buckets: 0..max_ms inclusive, plus overflow bucket
  explicit LatencyHistogram(std::int32_t max_ms = 5000);

  void add(std::int32_t latency_ms);

  // percentile in [0, 100], e.g. 50, 95, 99
  // Returns latency_ms estimate (bucket-based).
  std::int32_t percentile(double p) const;

  std::int64_t count() const { return total_; }

private:
  std::int32_t max_ms_;
  std::vector<std::int64_t> buckets_; // size max_ms_+2 (last is overflow)
  std::int64_t total_ = 0;
};

struct EndpointStats {
  std::int64_t count = 0;
  StatusBuckets status;
  LatencyHistogram latency;

  EndpointStats() : latency(5000) {}
};

class Aggregator {
public:
  void add(const Record& r);

  std::int64_t total_requests() const { return total_; }
  const StatusBuckets& total_status() const { return status_total_; }
  const LatencyHistogram& total_latency() const { return latency_total_; }

  const std::unordered_map<std::string, EndpointStats>& endpoints() const { return by_endpoint_; }

private:
  std::int64_t total_ = 0;
  StatusBuckets status_total_{};
  LatencyHistogram latency_total_{5000};

  std::unordered_map<std::string, EndpointStats> by_endpoint_;
};

} // namespace logstat
