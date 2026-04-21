//===----------------------------------------------------------------------===//
//                         DuckDB
//
// headless_duck_writer.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/copy_function.hpp"

namespace duckdb {

CopyFunction GetHeadlessDuckCopyFunction();

} // namespace duckdb