#include "headless_duck_metadata.hpp"

#include "headless_duck_block_manager.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group.hpp"

#include <cstring>
#include <map>

namespace duckdb {

HeadlessDuckFileMetadata ReadHeadlessDuckFileMetadata(ClientContext &context, const string &file_path) {
	HeadlessDuckFileMetadata result;
	result.file_path = file_path;

	auto &fs = FileSystem::GetFileSystem(context);
	auto meta_handle = fs.OpenFile(result.file_path, FileFlags::FILE_FLAGS_READ);
	result.file_size = fs.GetFileSize(*meta_handle);
	result.last_modified = fs.GetLastModifiedTime(*meta_handle);

	const idx_t minimum_size = sizeof(HeadlessDuckHeader) + sizeof(HeadlessDuckFooter);
	if (result.file_size < minimum_size) {
		throw IOException("headless_duck: file '%s' is %llu bytes, needs at least %llu", result.file_path,
		                  (unsigned long long)result.file_size, (unsigned long long)minimum_size);
	}

	HeadlessDuckHeader header;
	meta_handle->Read(&header, sizeof(header), 0);
	if (std::memcmp(header.magic, HEADLESS_DUCK_MAGIC, HEADLESS_DUCK_MAGIC_SIZE) != 0) {
		throw IOException("headless_duck: bad leading magic in %s", result.file_path);
	}
	if (header.format_version != HEADLESS_DUCK_FORMAT_VERSION) {
		throw IOException("headless_duck: unsupported format_version %u", header.format_version);
	}
	if (header.flags != 0) {
		throw IOException("headless_duck: reserved header flags must be 0");
	}

	meta_handle->Read(&result.footer, sizeof(result.footer), result.file_size - sizeof(result.footer));
	if (std::memcmp(result.footer.magic, HEADLESS_DUCK_MAGIC, HEADLESS_DUCK_MAGIC_SIZE) != 0) {
		throw IOException("headless_duck: bad trailing magic in %s", result.file_path);
	}
	if (result.footer.metadata_checksum != 0) {
		throw IOException("headless_duck: metadata_checksum is reserved and must be 0");
	}
	const auto footer_start = result.file_size - sizeof(result.footer);
	if (result.footer.metadata_offset > footer_start ||
	    result.footer.metadata_length > footer_start - result.footer.metadata_offset) {
		throw IOException("headless_duck: footer points outside the file");
	}

	vector<data_t> metadata_buffer(result.footer.metadata_length);
	meta_handle->Read(metadata_buffer.data(), result.footer.metadata_length, result.footer.metadata_offset);
	MemoryStream metadata_stream(metadata_buffer.data(), result.footer.metadata_length);

	try {
		BinaryDeserializer deserializer(metadata_stream);
		deserializer.Begin();
		const auto column_count = deserializer.ReadProperty<uint32_t>(100, "column_count");
		deserializer.ReadList(101, "columns", [&](Deserializer::List &list, idx_t) {
			list.ReadObject([&](Deserializer &obj) {
				result.column_names.push_back(obj.ReadProperty<string>(200, "name"));
				result.sql_types.push_back(obj.ReadProperty<LogicalType>(201, "type"));
			});
		});
		if (result.column_names.size() != column_count || result.sql_types.size() != column_count) {
			throw IOException("headless_duck: metadata column count mismatch");
		}
		deserializer.ReadList(103, "row_groups", [&](Deserializer::List &list, idx_t) {
			list.ReadObject(
			    [&](Deserializer &obj) { result.row_group_pointers.push_back(RowGroup::Deserialize(obj)); });
		});
		auto mm_size = deserializer.ReadProperty<uint64_t>(104, "metadata_manager_size");
		result.metadata_manager_bytes.resize(mm_size);
		deserializer.ReadProperty(105, "metadata_manager_bytes", result.metadata_manager_bytes.data(), mm_size);
		deserializer.End();
	} catch (SerializationException &ex) {
		throw IOException("headless_duck: failed to read metadata: %s", ex.what());
	}

	return result;
}

unique_ptr<HeadlessDuckBlockManager> OpenHeadlessDuckBlockManager(ClientContext &context,
                                                                  const HeadlessDuckFileMetadata &metadata) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto bm_handle = fs.OpenFile(metadata.file_path, FileFlags::FILE_FLAGS_READ);
	auto &db_instance = *context.db;
	auto result = make_uniq<HeadlessDuckBlockManager>(db_instance, BufferManager::GetBufferManager(db_instance),
	                                                  std::move(bm_handle), HeadlessDuckBlockManager::Mode::READ,
	                                                  /*base_offset*/ sizeof(HeadlessDuckHeader),
	                                                  /*block_alloc_size*/ metadata.footer.block_alloc_size,
	                                                  DEFAULT_BLOCK_HEADER_STORAGE_SIZE);
	result->SetBlockCount(metadata.footer.block_count);

	auto metadata_manager_bytes = metadata.metadata_manager_bytes;
	MemoryStream mm_stream(metadata_manager_bytes.data(), metadata_manager_bytes.size());
	try {
		result->GetMetadataManager().Read(mm_stream);
	} catch (SerializationException &ex) {
		throw IOException("headless_duck: failed to read metadata manager: %s", ex.what());
	}
	return result;
}

static PersistentColumnData ReadPersistentColumnData(HeadlessDuckBlockManager &block_manager, const LogicalType &type,
                                                     MetaBlockPointer pointer) {
	try {
		MetadataReader metadata_reader(block_manager.GetMetadataManager(), pointer);
		BinaryDeserializer deserializer(metadata_reader);
		deserializer.Set<const LogicalType &>(type);
		deserializer.Begin();
		auto result = PersistentColumnData::Deserialize(deserializer);
		deserializer.End();
		deserializer.Unset<LogicalType>();
		return result;
	} catch (SerializationException &ex) {
		throw IOException("headless_duck: failed to read column metadata: %s", ex.what());
	}
}

static bool GetExactNullCount(const BaseStatistics &stats, idx_t count, idx_t &null_count) {
	if (!stats.CanHaveNull()) {
		null_count = 0;
		return true;
	}
	if (!stats.CanHaveNoNull()) {
		null_count = count;
		return true;
	}
	return false;
}

static bool HasExactStringMinMax(const BaseStatistics &stats) {
	if (!StringStats::HasMinMax(stats) || !StringStats::HasMaxStringLength(stats)) {
		return false;
	}
	const auto max_length = StringStats::MaxStringLength(stats);
	return max_length == StringStats::Min(stats).length() && max_length == StringStats::Max(stats).length();
}

static Value GetStringStatsValue(const BaseStatistics &stats, bool get_min) {
	auto string_value = get_min ? StringStats::Min(stats) : StringStats::Max(stats);
	if (stats.GetType().id() == LogicalTypeId::BLOB) {
		return Value(Blob::ToString(string_value));
	}
	return Value(std::move(string_value));
}

static void AddMinMaxStatistics(const BaseStatistics &stats, case_insensitive_map_t<Value> &column_stats) {
	switch (stats.GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		if (NumericStats::HasMinMax(stats)) {
			column_stats["min"] = NumericStats::Min(stats);
			column_stats["max"] = NumericStats::Max(stats);
		}
		break;
	case StatisticsType::STRING_STATS:
		if (HasExactStringMinMax(stats)) {
			column_stats["min"] = GetStringStatsValue(stats, true);
			column_stats["max"] = GetStringStatsValue(stats, false);
		}
		break;
	default:
		break;
	}
}

void SetHeadlessDuckFileStatistics(CopyFunctionFileStatistics &statistics, idx_t file_size_bytes,
                                   const vector<string> &column_names, const vector<idx_t> &row_group_counts,
                                   const vector<vector<const BaseStatistics *>> &row_group_statistics) {
	statistics.row_count = 0;
	statistics.file_size_bytes = file_size_bytes;
	statistics.footer_size_bytes = Value::UBIGINT(sizeof(HeadlessDuckFooter));
	statistics.column_statistics.clear();

	for (const auto &row_group_count : row_group_counts) {
		statistics.row_count += row_group_count;
	}

	for (idx_t column_idx = 0; column_idx < column_names.size(); column_idx++) {
		case_insensitive_map_t<Value> column_stats;
		column_stats["num_values"] = Value::UBIGINT(statistics.row_count);

		unique_ptr<BaseStatistics> merged_stats;
		bool has_exact_null_count = true;
		idx_t null_count = 0;
		for (idx_t row_group_idx = 0; row_group_idx < row_group_statistics.size(); row_group_idx++) {
			if (column_idx >= row_group_statistics[row_group_idx].size() ||
			    !row_group_statistics[row_group_idx][column_idx]) {
				has_exact_null_count = false;
				continue;
			}
			const auto &stats = *row_group_statistics[row_group_idx][column_idx];
			if (!merged_stats) {
				merged_stats = stats.ToUnique();
			} else {
				merged_stats->Merge(stats);
			}

			idx_t row_group_null_count;
			if (GetExactNullCount(stats, row_group_counts[row_group_idx], row_group_null_count)) {
				null_count += row_group_null_count;
			} else {
				has_exact_null_count = false;
			}
		}

		if (has_exact_null_count) {
			column_stats["null_count"] = Value::UBIGINT(null_count);
		}
		if (merged_stats) {
			AddMinMaxStatistics(*merged_stats, column_stats);
		}
		statistics.column_statistics.emplace(column_names[column_idx], std::move(column_stats));
	}
}

unique_ptr<BaseStatistics> GetHeadlessDuckPersistentColumnStatistics(const PersistentColumnData &column_data) {
	auto result = BaseStatistics::CreateEmpty(column_data.logical_type).ToUnique();
	for (const auto &pointer : column_data.pointers) {
		result->Merge(pointer.statistics);
	}
	if (!column_data.child_columns.empty()) {
		// Validity columns merge their statistics into the parent column stats
		// during normal RowGroupCollection initialization. Mirror that here so
		// metadata-only stats keep the same nullability semantics.
		for (const auto &pointer : column_data.child_columns[0].pointers) {
			result->Merge(pointer.statistics);
		}
	}
	return result;
}

void ReadHeadlessDuckFileStatistics(ClientContext &context, const string &file_path,
                                    CopyFunctionFileStatistics &statistics) {
	auto metadata = ReadHeadlessDuckFileMetadata(context, file_path);
	auto block_manager = OpenHeadlessDuckBlockManager(context, metadata);

	vector<idx_t> row_group_counts;
	vector<vector<unique_ptr<BaseStatistics>>> owned_statistics;
	vector<vector<const BaseStatistics *>> row_group_statistics;
	row_group_counts.reserve(metadata.row_group_pointers.size());
	owned_statistics.reserve(metadata.row_group_pointers.size());
	row_group_statistics.reserve(metadata.row_group_pointers.size());

	for (auto &row_group : metadata.row_group_pointers) {
		row_group_counts.push_back(row_group.tuple_count);
		vector<unique_ptr<BaseStatistics>> row_group_owned_statistics;
		vector<const BaseStatistics *> row_group_stat_refs;
		row_group_owned_statistics.reserve(metadata.sql_types.size());
		row_group_stat_refs.reserve(metadata.sql_types.size());

		if (row_group.data_pointers.size() != metadata.sql_types.size()) {
			throw IOException("headless_duck: row group column count mismatch");
		}
		for (idx_t column_idx = 0; column_idx < row_group.data_pointers.size(); column_idx++) {
			auto column_data = ReadPersistentColumnData(*block_manager, metadata.sql_types[column_idx],
			                                            row_group.data_pointers[column_idx]);
			auto column_stats = GetHeadlessDuckPersistentColumnStatistics(column_data);
			row_group_stat_refs.push_back(column_stats.get());
			row_group_owned_statistics.push_back(std::move(column_stats));
		}

		row_group_statistics.push_back(std::move(row_group_stat_refs));
		owned_statistics.push_back(std::move(row_group_owned_statistics));
	}

	SetHeadlessDuckFileStatistics(statistics, metadata.file_size, metadata.column_names, row_group_counts,
	                              row_group_statistics);
}

static Value
CreateColumnStatisticsValue(const case_insensitive_map_t<case_insensitive_map_t<Value>> &column_statistics) {
	map<string, Value> ordered_column_stats;
	for (auto &column_entry : column_statistics) {
		map<string, Value> ordered_stats;
		for (auto &stats_entry : column_entry.second) {
			ordered_stats.emplace(stats_entry.first, stats_entry.second);
		}

		vector<Value> stats_keys;
		vector<Value> stats_values;
		for (auto &stats_entry : ordered_stats) {
			stats_keys.emplace_back(stats_entry.first);
			stats_values.emplace_back(std::move(stats_entry.second));
		}
		auto stats_map =
		    Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(stats_keys), std::move(stats_values));
		ordered_column_stats.emplace(column_entry.first, std::move(stats_map));
	}

	vector<Value> column_keys;
	vector<Value> column_values;
	for (auto &column_entry : ordered_column_stats) {
		column_keys.emplace_back(column_entry.first);
		column_values.emplace_back(std::move(column_entry.second));
	}
	auto map_value_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	return Value::MAP(LogicalType::VARCHAR, map_value_type, std::move(column_keys), std::move(column_values));
}

struct HeadlessDuckFileStatsBindData : public TableFunctionData {
	string file_path;
	CopyFunctionFileStatistics statistics;
};

struct HeadlessDuckFileStatsGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> HeadlessDuckFileStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HeadlessDuckFileStatsBindData>();
	result->file_path = input.inputs[0].GetValue<string>();
	ReadHeadlessDuckFileStatistics(context, result->file_path, result->statistics);

	return_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	names = GetCopyFunctionReturnNames(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> HeadlessDuckFileStatsInitGlobal(ClientContext &context,
                                                                            TableFunctionInitInput &input) {
	return make_uniq<HeadlessDuckFileStatsGlobalState>();
}

static void HeadlessDuckFileStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HeadlessDuckFileStatsGlobalState>();
	if (state.finished) {
		output.SetCardinality(0);
		return;
	}
	auto &bind = data.bind_data->Cast<HeadlessDuckFileStatsBindData>();
	const auto &statistics = bind.statistics;

	output.SetCardinality(1);
	output.data[0].Append(Value(bind.file_path));
	output.data[1].Append(Value::UBIGINT(statistics.row_count));
	output.data[2].Append(Value::UBIGINT(statistics.file_size_bytes));
	output.data[3].Append(statistics.footer_size_bytes);
	output.data[4].Append(CreateColumnStatisticsValue(statistics.column_statistics));
	output.data[5].Append(Value(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));
	state.finished = true;
}

TableFunction GetHeadlessDuckFileStatsFunction() {
	TableFunction function("headlessduck_file_stats", {LogicalType::VARCHAR}, HeadlessDuckFileStatsFunction,
	                       HeadlessDuckFileStatsBind, HeadlessDuckFileStatsInitGlobal);
	return function;
}

struct HeadlessDuckStorageSegmentInfo {
	idx_t row_group_index;
	string column_name;
	idx_t column_id;
	string column_path;
	idx_t segment_idx;
	string segment_type;
	idx_t segment_start;
	idx_t segment_count;
	string compression_type;
	Value segment_stats;
	bool persistent;
	block_id_t block_id;
	idx_t block_offset;
	string segment_info;
	vector<block_id_t> additional_blocks;
};

struct HeadlessDuckStorageInfoBindData : public TableFunctionData {
	string file_path;
	vector<HeadlessDuckStorageSegmentInfo> segment_info;
};

struct HeadlessDuckStorageInfoGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

static void AddHeadlessDuckStorageInfoColumns(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("row_group_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("column_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("column_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("column_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("segment_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("segment_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("start");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("compression");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("stats");
	return_types.emplace_back(LogicalType::VARIANT());
	names.emplace_back("has_updates");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("persistent");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("block_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("block_offset");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("segment_info");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("additional_block_ids");
	return_types.emplace_back(LogicalType::LIST(LogicalTypeId::BIGINT));
}

static string HeadlessDuckColumnPathToString(const vector<idx_t> &column_path) {
	string result = "[";
	for (idx_t i = 0; i < column_path.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += to_string(column_path[i]);
	}
	result += "]";
	return result;
}

static void AddHeadlessDuckPersistentColumnInfo(vector<HeadlessDuckStorageSegmentInfo> &result,
                                                const PersistentColumnData &column_data, const string &column_name,
                                                idx_t column_id, vector<idx_t> column_path, idx_t row_group_index) {
	auto column_path_string = HeadlessDuckColumnPathToString(column_path);
	for (idx_t segment_idx = 0; segment_idx < column_data.pointers.size(); segment_idx++) {
		auto &pointer = column_data.pointers[segment_idx];
		HeadlessDuckStorageSegmentInfo info;
		info.row_group_index = row_group_index;
		info.column_name = column_name;
		info.column_id = column_id;
		info.column_path = column_path_string;
		info.segment_idx = segment_idx;
		info.segment_type = column_data.logical_type.ToString();
		info.segment_start = pointer.row_start;
		info.segment_count = pointer.tuple_count;
		info.compression_type = CompressionTypeToString(pointer.compression_type);
		info.segment_stats = pointer.statistics.ToStruct();
		info.persistent = true;
		info.block_id = pointer.block_pointer.block_id;
		info.block_offset = pointer.block_pointer.offset;
		if (pointer.segment_state) {
			info.additional_blocks = pointer.segment_state->blocks;
		}
		result.push_back(std::move(info));
	}
	for (idx_t child_idx = 0; child_idx < column_data.child_columns.size(); child_idx++) {
		auto child_path = column_path;
		child_path.push_back(child_idx);
		AddHeadlessDuckPersistentColumnInfo(result, column_data.child_columns[child_idx], column_name, column_id,
		                                    std::move(child_path), row_group_index);
	}
}

static unique_ptr<FunctionData> HeadlessDuckStorageInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HeadlessDuckStorageInfoBindData>();
	result->file_path = input.inputs[0].GetValue<string>();
	AddHeadlessDuckStorageInfoColumns(return_types, names);

	auto metadata = ReadHeadlessDuckFileMetadata(context, result->file_path);
	auto block_manager = OpenHeadlessDuckBlockManager(context, metadata);
	for (idx_t row_group_idx = 0; row_group_idx < metadata.row_group_pointers.size(); row_group_idx++) {
		auto &row_group = metadata.row_group_pointers[row_group_idx];
		if (row_group.data_pointers.size() != metadata.sql_types.size()) {
			throw IOException("headless_duck: row group column count mismatch");
		}
		for (idx_t column_idx = 0; column_idx < row_group.data_pointers.size(); column_idx++) {
			auto column_data = ReadPersistentColumnData(*block_manager, metadata.sql_types[column_idx],
			                                            row_group.data_pointers[column_idx]);
			AddHeadlessDuckPersistentColumnInfo(result->segment_info, column_data, metadata.column_names[column_idx],
			                                    column_idx, {column_idx}, row_group_idx);
		}
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> HeadlessDuckStorageInfoInitGlobal(ClientContext &context,
                                                                              TableFunctionInitInput &input) {
	return make_uniq<HeadlessDuckStorageInfoGlobalState>();
}

static Value HeadlessDuckValueFromBlockIdList(const vector<block_id_t> &block_ids) {
	vector<Value> blocks;
	for (auto &block_id : block_ids) {
		blocks.push_back(Value::BIGINT(block_id));
	}
	return Value::LIST(LogicalTypeId::BIGINT, blocks);
}

static void HeadlessDuckStorageInfoFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<HeadlessDuckStorageInfoBindData>();
	auto &state = data_p.global_state->Cast<HeadlessDuckStorageInfoGlobalState>();
	idx_t count = 0;

	while (state.offset < bind_data.segment_info.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.segment_info[state.offset++];
		output.data[0].Append(Value::BIGINT(NumericCast<int64_t>(entry.row_group_index)));
		output.data[1].Append(Value(entry.column_name));
		output.data[2].Append(Value::BIGINT(NumericCast<int64_t>(entry.column_id)));
		output.data[3].Append(Value(entry.column_path));
		output.data[4].Append(Value::BIGINT(NumericCast<int64_t>(entry.segment_idx)));
		output.data[5].Append(Value(entry.segment_type));
		output.data[6].Append(Value::BIGINT(NumericCast<int64_t>(entry.segment_start)));
		output.data[7].Append(Value::BIGINT(NumericCast<int64_t>(entry.segment_count)));
		output.data[8].Append(Value(entry.compression_type));
		output.data[9].Append(entry.segment_stats);
		output.data[10].Append(Value::BOOLEAN(false));
		output.data[11].Append(Value::BOOLEAN(entry.persistent));
		output.data[12].Append(Value::BIGINT(entry.block_id));
		output.data[13].Append(Value::BIGINT(NumericCast<int64_t>(entry.block_offset)));
		output.data[14].Append(Value(entry.segment_info));
		output.data[15].Append(HeadlessDuckValueFromBlockIdList(entry.additional_blocks));
		count++;
	}
	output.SetCardinality(count);
}

TableFunction GetHeadlessDuckStorageInfoFunction() {
	TableFunction function("headlessduck_storage_info", {LogicalType::VARCHAR}, HeadlessDuckStorageInfoFunction,
	                       HeadlessDuckStorageInfoBind, HeadlessDuckStorageInfoInitGlobal);
	return function;
}

} // namespace duckdb
