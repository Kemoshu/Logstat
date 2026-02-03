#pragma once

#include <functional>
#include <string>

#include "logstat/types.hpp"

namespace logstat {

bool parse_csv_file(const std::string& path,
                    const std::function<void(const Record&)>& on_record,
                    std::string* error_out = nullptr);

} // namespace logstat
