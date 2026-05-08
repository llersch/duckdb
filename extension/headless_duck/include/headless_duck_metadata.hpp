//===----------------------------------------------------------------------===//
//                         DuckDB
//
// headless_duck_metadata.hpp
//
// Shared .hduck metadata parsing and statistics helpers.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "headless_duck_format.hpp"

#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/data_pointer.hpp"

namespace duckdb {

class BaseStatistics;
class HeadlessDuckBlockManager;

struct HeadlessDuckFileMetadata {
	string file_path;
	idx_t file_size;
	HeadlessDuckFooter footer;
	vector<string> column_names;
	vector<LogicalType> sql_types;
	vector<RowGroupPointer> row_group_pointers;
	vector<data_t> metadata_manager_bytes;
};

HeadlessDuckFileMetadata ReadHeadlessDuckFileMetadata(ClientContext &context, const string &file_path);

unique_ptr<HeadlessDuckBlockManager> OpenHeadlessDuckBlockManager(ClientContext &context,
                                                                  const HeadlessDuckFileMetadata &metadata);

void SetHeadlessDuckFileStatistics(CopyFunctionFileStatistics &statistics, idx_t file_size_bytes,
                                   const vector<string> &column_names, const vector<idx_t> &row_group_counts,
                                   const vector<vector<const BaseStatistics *>> &row_group_statistics);

void ReadHeadlessDuckFileStatistics(ClientContext &context, const string &file_path,
                                    CopyFunctionFileStatistics &statistics);

TableFunction GetHeadlessDuckFileStatsFunction();

} // namespace duckdb
