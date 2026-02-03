#include "logstat/aggregator.hpp"

#include <algorithm>
#include <cmath>

namespace logstat {

void StatusBuckets::add(std::int32_t status) {
  if (status >= 200 && status <= 299) ++s2xx;
  else if (status >= 300 && status <= 399) ++s3xx;
  else if (status >= 400 && status <= 499) ++s4xx;
  else if (status >= 500 && status <= 599) ++s5xx;
  else ++other;
}

LatencyHistogram::LatencyHistogram(std::int32_t max_ms)
    : max_ms_(max_ms),
      buckets_(static_cast<size_t>(max_ms_) + 2, 0),
      total_(0) {}

void LatencyHistogram::add(std::int32_t latency_ms) {
  if (latency_ms < 0) latency_ms = 0;

  size_t idx = 0;
  if (latency_ms > max_ms_) {
    idx = buckets_.size() - 1; // overflow
  } else {
    idx = static_cast<size_t>(latency_ms);
  }

  ++buckets_[idx];
  ++total_;
}

std::int32_t LatencyHistogram::percentile(double p) const {
  if (total_ == 0) return 0;
  if (p <= 0.0) return 0;
  if (p >= 100.0) return max_ms_;

  // target rank (1-based-ish): smallest latency where cumulative >= target
  const double target_d = std::ceil((p / 100.0) * static_cast<double>(total_));
  const std::int64_t target = static_cast<std::int64_t>(std::max(1.0, target_d));

  std::int64_t cum = 0;
  for (size_t i = 0; i < buckets_.size(); ++i) {
    cum += buckets_[i];
    if (cum >= target) {
      if (i == buckets_.size() - 1) return max_ms_; // overflow: clamp
      return static_cast<std::int32_t>(i);
    }
  }
  return max_ms_;
}

void Aggregator::add(const Record& r) {
  ++total_;
  status_total_.add(r.status);
  latency_total_.add(r.latency_ms);

  auto& ep = by_endpoint_[r.endpoint];
  ++ep.count;
  ep.status.add(r.status);
  ep.latency.add(r.latency_ms);
}

} // namespace logstat
