#include "headless_duck_reader.hpp"

#include "headless_duck_block_manager.hpp"
#include "headless_duck_metadata.hpp"

#include "duckdb/common/enums/scan_options.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include <iostream>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind data — produced by the bind callback, read by the scan callback
//===----------------------------------------------------------------------===//

struct HeadlessDuckReadBindData : public TableFunctionData {
	string file_path;
	vector<string> column_names;
	vector<LogicalType> sql_types;
	HeadlessDuckFileMetadata metadata;
	idx_t row_count = 0;
	mutable vector<unique_ptr<BaseStatistics>> column_statistics;
	mutable vector<bool> column_statistics_loaded;
	mutable vector<vector<unique_ptr<PersistentColumnData>>> column_metadata_cache;
	mutable vector<shared_ptr<PartitionRowGroup>> partition_row_groups;
	mutable bool partition_row_groups_loaded = false;
};

//===----------------------------------------------------------------------===//
// Global state — tracks whether we've emitted our single status row yet
//===----------------------------------------------------------------------===//

struct HeadlessDuckReadGlobalState : public GlobalTableFunctionState {
	unique_ptr<HeadlessDuckBlockManager> block_manager;
	shared_ptr<HeadlessDuckTableIOManager> table_io;
	shared_ptr<DataTableInfo> data_table_info;
	unique_ptr<RowGroupCollection> rg_collection;
	ParallelCollectionScanState parallel_state;
	vector<column_t> output_projection_ids;
	idx_t max_threads = 1;

	idx_t MaxThreads() const override {
		return max_threads;
	}

	bool CanRemoveFilterColumns() const {
		return !output_projection_ids.empty();
	}
};

struct HeadlessDuckReadLocalState : public LocalTableFunctionState {
	TableScanState scan_state;
	DataChunk all_columns;
	idx_t rows_in_current_row_group = 0;
};

struct HeadlessDuckProjection {
	vector<idx_t> source_column_ids;
	vector<LogicalType> source_types;
	vector<StorageIndex> scan_column_ids;
	vector<LogicalType> scan_types;
};

//===----------------------------------------------------------------------===//
// Bind: open file, validate envelope, report schema
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> HeadlessDuckReadBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HeadlessDuckReadBindData>();
	result->file_path = input.inputs[0].GetValue<string>();
	result->metadata = ReadHeadlessDuckFileMetadata(context, result->file_path);
	result->column_names = result->metadata.column_names;
	result->sql_types = result->metadata.sql_types;
	for (auto &row_group : result->metadata.row_group_pointers) {
		result->row_count += row_group.tuple_count;
	}
	result->column_statistics.resize(result->sql_types.size());
	result->column_statistics_loaded.resize(result->sql_types.size(), false);
	result->column_metadata_cache.resize(result->sql_types.size());

	return_types = result->sql_types;
	names = result->column_names;
	return std::move(result);
}

static idx_t GetOrCreateLocalColumn(HeadlessDuckProjection &projection, const HeadlessDuckReadBindData &bind,
                                    idx_t source_column_id) {
	for (idx_t local_idx = 0; local_idx < projection.source_column_ids.size(); local_idx++) {
		if (projection.source_column_ids[local_idx] == source_column_id) {
			return local_idx;
		}
	}
	projection.source_column_ids.push_back(source_column_id);
	projection.source_types.push_back(bind.sql_types[source_column_id]);
	return projection.source_column_ids.size() - 1;
}

static const LogicalType &GetScanType(const HeadlessDuckReadBindData &bind, const ColumnIndex &column_index) {
	if (column_index.HasType()) {
		return column_index.GetScanType();
	}
	return bind.sql_types[column_index.GetPrimaryIndex()];
}

static HeadlessDuckProjection GetProjection(const HeadlessDuckReadBindData &bind, TableFunctionInitInput &input) {
	HeadlessDuckProjection projection;
	projection.scan_column_ids.reserve(input.column_indexes.size());
	projection.scan_types.reserve(input.column_indexes.size());
	for (auto &column_index : input.column_indexes) {
		if (!column_index.HasPrimaryIndex()) {
			throw IOException("headless_duck: field-name projections are not supported");
		}
		auto source_column_id = column_index.GetPrimaryIndex();
		if (source_column_id >= bind.sql_types.size()) {
			throw IOException("headless_duck: requested column index %llu is outside the file schema",
			                  (unsigned long long)source_column_id);
		}
		auto local_column_id = GetOrCreateLocalColumn(projection, bind, source_column_id);
		auto storage_index = StorageIndex::FromColumnIndex(column_index);
		storage_index.SetIndex(local_column_id);
		projection.scan_column_ids.push_back(std::move(storage_index));
		projection.scan_types.push_back(GetScanType(bind, column_index));
	}
	if (projection.scan_column_ids.empty() && !bind.sql_types.empty()) {
		projection.source_column_ids.push_back(0);
		projection.source_types.push_back(bind.sql_types[0]);
		projection.scan_column_ids.emplace_back(0);
		projection.scan_types.push_back(bind.sql_types[0]);
	}
	return projection;
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

static bool ColumnMetadataCacheComplete(const HeadlessDuckReadBindData &bind, idx_t column_idx) {
	if (column_idx >= bind.column_metadata_cache.size()) {
		return false;
	}
	auto &cached_column = bind.column_metadata_cache[column_idx];
	if (cached_column.size() != bind.metadata.row_group_pointers.size()) {
		return false;
	}
	for (auto &row_group_column_data : cached_column) {
		if (!row_group_column_data) {
			return false;
		}
	}
	return true;
}

static void LoadColumnMetadataCache(HeadlessDuckBlockManager &block_manager, HeadlessDuckReadBindData &bind,
                                    idx_t column_idx) {
	if (ColumnMetadataCacheComplete(bind, column_idx)) {
		return;
	}

	vector<unique_ptr<PersistentColumnData>> cached_column;
	cached_column.reserve(bind.metadata.row_group_pointers.size());
	for (auto &row_group : bind.metadata.row_group_pointers) {
		if (column_idx >= row_group.data_pointers.size()) {
			throw IOException("headless_duck: row group column count mismatch");
		}
		auto column_data =
		    ReadPersistentColumnData(block_manager, bind.sql_types[column_idx], row_group.data_pointers[column_idx]);
		cached_column.push_back(make_uniq<PersistentColumnData>(std::move(column_data)));
	}
	bind.column_metadata_cache[column_idx] = std::move(cached_column);
}

static void LoadColumnMetadataCache(ClientContext &context, HeadlessDuckReadBindData &bind, idx_t column_idx) {
	auto block_manager = OpenHeadlessDuckBlockManager(context, bind.metadata);
	LoadColumnMetadataCache(*block_manager, bind, column_idx);
}

static unique_ptr<NodeStatistics> HeadlessDuckReadCardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &bind = bind_data->Cast<HeadlessDuckReadBindData>();
	return make_uniq<NodeStatistics>(bind.row_count, bind.row_count);
}

static unique_ptr<BaseStatistics> HeadlessDuckReadStatistics(ClientContext &context,
                                                             TableFunctionGetStatisticsInput &input) {
	auto &bind = input.bind_data->CastNoConst<HeadlessDuckReadBindData>();
	if (!input.column_index.HasPrimaryIndex()) {
		return nullptr;
	}
	const auto column_idx = input.column_index.GetPrimaryIndex();
	if (column_idx >= bind.sql_types.size()) {
		return nullptr;
	}

	if (!bind.column_statistics_loaded[column_idx]) {
		LoadColumnMetadataCache(context, bind, column_idx);
		unique_ptr<BaseStatistics> merged_stats;

		for (auto &column_data : bind.column_metadata_cache[column_idx]) {
			auto column_stats = GetHeadlessDuckPersistentColumnStatistics(*column_data);
			if (!merged_stats) {
				merged_stats = std::move(column_stats);
			} else {
				merged_stats->Merge(*column_stats);
			}
		}

		if (!merged_stats) {
			merged_stats = BaseStatistics::CreateEmpty(bind.sql_types[column_idx]).ToUnique();
		}
		bind.column_statistics[column_idx] = std::move(merged_stats);
		bind.column_statistics_loaded[column_idx] = true;
	}
	return bind.column_statistics[column_idx] ? bind.column_statistics[column_idx]->ToUnique() : nullptr;
}

struct HeadlessDuckPartitionRowGroup : public PartitionRowGroup {
	explicit HeadlessDuckPartitionRowGroup(vector<unique_ptr<BaseStatistics>> column_statistics_p)
	    : column_statistics(std::move(column_statistics_p)) {
	}

	vector<unique_ptr<BaseStatistics>> column_statistics;

	unique_ptr<BaseStatistics> GetColumnStatistics(const StorageIndex &storage_index) override {
		if (!storage_index.HasPrimaryIndex()) {
			return nullptr;
		}
		const auto column_idx = storage_index.GetPrimaryIndex();
		if (column_idx >= column_statistics.size() || !column_statistics[column_idx]) {
			return nullptr;
		}
		if (storage_index.HasChildren()) {
			try {
				return column_statistics[column_idx]->PushdownExtract(storage_index);
			} catch (InternalException &) {
				return nullptr;
			}
		}
		return column_statistics[column_idx]->ToUnique();
	}

	bool MinMaxIsExact(const BaseStatistics &stats, const StorageIndex &) override {
		if (stats.GetStatsType() == StatisticsType::STRING_STATS) {
			if (!StringStats::HasMinMax(stats) || !StringStats::HasMaxStringLength(stats)) {
				return false;
			}
			const auto max_length = StringStats::MaxStringLength(stats);
			return max_length == StringStats::Max(stats).length() && max_length == StringStats::Min(stats).length();
		}
		return stats.GetStatsType() == StatisticsType::NUMERIC_STATS;
	}
};

static void LoadPartitionRowGroups(ClientContext &context, HeadlessDuckReadBindData &bind) {
	if (bind.partition_row_groups_loaded) {
		return;
	}

	auto block_manager = OpenHeadlessDuckBlockManager(context, bind.metadata);
	for (idx_t column_idx = 0; column_idx < bind.sql_types.size(); column_idx++) {
		LoadColumnMetadataCache(*block_manager, bind, column_idx);
	}

	bind.partition_row_groups.clear();
	bind.partition_row_groups.reserve(bind.metadata.row_group_pointers.size());
	for (idx_t row_group_idx = 0; row_group_idx < bind.metadata.row_group_pointers.size(); row_group_idx++) {
		vector<unique_ptr<BaseStatistics>> row_group_statistics;
		row_group_statistics.reserve(bind.sql_types.size());
		for (idx_t column_idx = 0; column_idx < bind.sql_types.size(); column_idx++) {
			row_group_statistics.push_back(
			    GetHeadlessDuckPersistentColumnStatistics(*bind.column_metadata_cache[column_idx][row_group_idx]));
		}
		bind.partition_row_groups.push_back(
		    make_shared_ptr<HeadlessDuckPartitionRowGroup>(std::move(row_group_statistics)));
	}
	bind.partition_row_groups_loaded = true;
}

static vector<PartitionStatistics> HeadlessDuckReadPartitionStats(ClientContext &context, GetPartitionStatsInput &input) {
	auto &bind = input.bind_data->CastNoConst<HeadlessDuckReadBindData>();
	LoadPartitionRowGroups(context, bind);

	vector<PartitionStatistics> result;
	result.reserve(bind.metadata.row_group_pointers.size());
	for (idx_t row_group_idx = 0; row_group_idx < bind.metadata.row_group_pointers.size(); row_group_idx++) {
		auto &row_group = bind.metadata.row_group_pointers[row_group_idx];
		PartitionStatistics partition_stats;
		partition_stats.row_start = row_group.row_start;
		partition_stats.count = row_group.tuple_count;
		partition_stats.count_type = CountType::COUNT_EXACT;
		partition_stats.partition_row_group = bind.partition_row_groups[row_group_idx];
		result.push_back(std::move(partition_stats));
	}
	return result;
}

static PersistentColumnData GetCachedOrReadPersistentColumnData(HeadlessDuckBlockManager &block_manager,
                                                                const HeadlessDuckReadBindData &bind, idx_t row_group_idx,
                                                                idx_t column_id) {
	if (ColumnMetadataCacheComplete(bind, column_id)) {
		auto result = std::move(*bind.column_metadata_cache[column_id][row_group_idx]);
		bind.column_metadata_cache[column_id][row_group_idx].reset();
		return result;
	}
	return ReadPersistentColumnData(block_manager, bind.sql_types[column_id],
	                                bind.metadata.row_group_pointers[row_group_idx].data_pointers[column_id]);
}

static PersistentCollectionData BuildProjectedCollectionData(HeadlessDuckBlockManager &block_manager,
                                                             const HeadlessDuckReadBindData &bind,
                                                             const vector<idx_t> &source_column_ids) {
	PersistentCollectionData persistent_collection;
	for (idx_t row_group_idx = 0; row_group_idx < bind.metadata.row_group_pointers.size(); row_group_idx++) {
		auto &rgp = bind.metadata.row_group_pointers[row_group_idx];
		PersistentRowGroupData prg;
		prg.start = rgp.row_start;
		prg.count = rgp.tuple_count;
		for (auto column_id : source_column_ids) {
			if (column_id >= rgp.data_pointers.size()) {
				throw IOException("headless_duck: row group column count mismatch");
			}
			const auto &column_type = bind.sql_types[column_id];
			prg.types.push_back(column_type);
			prg.column_data.push_back(GetCachedOrReadPersistentColumnData(block_manager, bind, row_group_idx, column_id));
		}
		persistent_collection.row_group_data.push_back(std::move(prg));
	}
	return persistent_collection;
}

//===----------------------------------------------------------------------===//
// Init / scan
//===----------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> HeadlessDuckReadInitGlobal(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<HeadlessDuckReadBindData>();
	auto state = make_uniq<HeadlessDuckReadGlobalState>();
	state->block_manager = OpenHeadlessDuckBlockManager(context, bind.metadata);
	state->table_io = make_shared_ptr<HeadlessDuckTableIOManager>(*state->block_manager, DEFAULT_ROW_GROUP_SIZE);

	auto &db_instance = *context.db;
	const auto &default_name = DatabaseManager::GetDefaultDatabase(context);
	auto attached = DatabaseManager::Get(db_instance).GetDatabase(context, default_name);
	if (!attached) {
		throw IOException("headless_duck: no default attached database");
	}
	state->data_table_info = make_shared_ptr<DataTableInfo>(*attached, state->table_io, "hduck", "read");

	auto projection = GetProjection(bind, input);

	auto persistent_collection = BuildProjectedCollectionData(*state->block_manager, bind, projection.source_column_ids);
	state->rg_collection =
	    make_uniq<RowGroupCollection>(state->data_table_info, *state->block_manager, projection.source_types,
	                                  /*row_start*/ 0, /*total_rows*/ 0, DEFAULT_ROW_GROUP_SIZE);
	state->rg_collection->Initialize(persistent_collection);

	if (input.CanRemoveFilterColumns()) {
		state->output_projection_ids = input.projection_ids;
	}

	state->rg_collection->InitializeParallelScan(state->parallel_state);
	idx_t parallel_scan_tuple_count = DEFAULT_ROW_GROUP_SIZE;
	if (ClientConfig::GetConfig(context).verify_parallelism) {
		parallel_scan_tuple_count = STANDARD_VECTOR_SIZE;
	}
	state->max_threads = MaxValue<idx_t>(1, state->rg_collection->GetTotalRows() / parallel_scan_tuple_count + 1);
	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> HeadlessDuckReadInitLocal(ExecutionContext &context,
                                                                     TableFunctionInitInput &input,
                                                                     GlobalTableFunctionState *global_state) {
	auto &bind = input.bind_data->Cast<HeadlessDuckReadBindData>();
	auto &gstate = global_state->Cast<HeadlessDuckReadGlobalState>();
	auto lstate = make_uniq<HeadlessDuckReadLocalState>();

	auto projection = GetProjection(bind, input);
	if (input.CanRemoveFilterColumns()) {
		lstate->all_columns.Initialize(context.client, projection.scan_types);
	}
	lstate->scan_state.Initialize(std::move(projection.scan_column_ids), context.client, input.filters, nullptr);
	lstate->rows_in_current_row_group =
	    gstate.rg_collection->NextParallelScan(context.client, gstate.parallel_state, lstate->scan_state.table_state);
	return std::move(lstate);
}

static bool HeadlessDuckReadScanCurrentRowGroup(HeadlessDuckReadGlobalState &gstate, HeadlessDuckReadLocalState &lstate,
                                                DataChunk &output) {
	auto &scan_output = gstate.CanRemoveFilterColumns() ? lstate.all_columns : output;
	if (gstate.CanRemoveFilterColumns()) {
		lstate.all_columns.Reset();
	}
	if (!lstate.scan_state.table_state.Scan(scan_output, TableScanType::TABLE_SCAN_COMMITTED_ROWS)) {
		return false;
	}
	if (gstate.CanRemoveFilterColumns()) {
		output.ReferenceColumns(lstate.all_columns, gstate.output_projection_ids);
	}
	return true;
}

static void HeadlessDuckReadFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<HeadlessDuckReadGlobalState>();
	auto &lstate = data.local_state->Cast<HeadlessDuckReadLocalState>();

	do {
		if (HeadlessDuckReadScanCurrentRowGroup(gstate, lstate, output)) {
			return;
		}

		lstate.rows_in_current_row_group =
		    gstate.rg_collection->NextParallelScan(context, gstate.parallel_state, lstate.scan_state.table_state);
		if (data.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
			data.async_result =
			    lstate.rows_in_current_row_group == 0 ? AsyncResultType::FINISHED : AsyncResultType::HAVE_MORE_OUTPUT;
			return;
		}
		if (lstate.rows_in_current_row_group == 0) {
			output.SetCardinality(0);
			return;
		}

		context.InterruptCheck();
	} while (true);
}

static void HeadlessDuckReadPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *,
                                                  vector<unique_ptr<Expression>> &filters) {
	FilterCombiner combiner(context);
	for (auto &filter : filters) {
		combiner.AddFilter(filter->Copy());
	}

	vector<FilterPushdownResult> pushdown_results;
	auto table_filters = combiner.GenerateTableScanFilters(get.GetColumnIds(), pushdown_results);
	for (auto &entry : table_filters) {
		auto optional_filter = make_uniq<OptionalFilter>(entry.TakeFilter());
		get.table_filters.PushFilter(entry.GetIndex(), std::move(optional_filter));
	}
}

//===----------------------------------------------------------------------===//
// Factory
//===----------------------------------------------------------------------===//

TableFunction GetHeadlessDuckReadFunction() {
	TableFunction function("read_headlessduck", {LogicalType::VARCHAR}, HeadlessDuckReadFunction, HeadlessDuckReadBind,
	                       HeadlessDuckReadInitGlobal, HeadlessDuckReadInitLocal);
	function.projection_pushdown = true;
	function.statistics_extended = HeadlessDuckReadStatistics;
	function.cardinality = HeadlessDuckReadCardinality;
	function.get_partition_stats = HeadlessDuckReadPartitionStats;
	function.pushdown_complex_filter = HeadlessDuckReadPushdownComplexFilter;
	function.filter_pushdown = true;
	function.filter_prune = true;
	function.order_preservation_type = OrderPreservationType::NO_ORDER;
	return function;
}

} // namespace duckdb
