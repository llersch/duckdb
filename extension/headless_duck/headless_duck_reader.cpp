#include "headless_duck_reader.hpp"

#include "headless_duck_block_manager.hpp"
#include "headless_duck_metadata.hpp"

#include "duckdb/common/enums/scan_options.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
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
};

//===----------------------------------------------------------------------===//
// Global state — tracks whether we've emitted our single status row yet
//===----------------------------------------------------------------------===//

struct HeadlessDuckReadGlobalState : public GlobalTableFunctionState {
	unique_ptr<HeadlessDuckBlockManager> block_manager;
	shared_ptr<HeadlessDuckTableIOManager> table_io;
	shared_ptr<DataTableInfo> data_table_info;
	unique_ptr<RowGroupCollection> rg_collection;
	unique_ptr<TableScanState> scan_state;
	DataChunk all_columns;
	vector<column_t> output_projection_ids;

	idx_t MaxThreads() const override {
		return 1;
	}

	bool CanRemoveFilterColumns() const {
		return !output_projection_ids.empty();
	}
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
	MetadataReader metadata_reader(block_manager.GetMetadataManager(), pointer);
	BinaryDeserializer deserializer(metadata_reader);
	deserializer.Set<const LogicalType &>(type);
	deserializer.Begin();
	auto result = PersistentColumnData::Deserialize(deserializer);
	deserializer.End();
	deserializer.Unset<LogicalType>();
	return result;
}

static PersistentCollectionData BuildProjectedCollectionData(HeadlessDuckBlockManager &block_manager,
                                                             const HeadlessDuckFileMetadata &metadata,
                                                             const vector<idx_t> &source_column_ids) {
	PersistentCollectionData persistent_collection;
	for (auto &rgp : metadata.row_group_pointers) {
		PersistentRowGroupData prg;
		prg.start = rgp.row_start;
		prg.count = rgp.tuple_count;
		for (auto column_id : source_column_ids) {
			if (column_id >= rgp.data_pointers.size()) {
				throw IOException("headless_duck: row group column count mismatch");
			}
			const auto &column_type = metadata.sql_types[column_id];
			prg.types.push_back(column_type);
			prg.column_data.push_back(
			    ReadPersistentColumnData(block_manager, column_type, rgp.data_pointers[column_id]));
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
	state->scan_state = make_uniq<TableScanState>();
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

	auto persistent_collection =
	    BuildProjectedCollectionData(*state->block_manager, bind.metadata, projection.source_column_ids);
	state->rg_collection =
	    make_uniq<RowGroupCollection>(state->data_table_info, *state->block_manager, projection.source_types,
	                                  /*row_start*/ 0, /*total_rows*/ 0, DEFAULT_ROW_GROUP_SIZE);
	state->rg_collection->Initialize(persistent_collection);

	if (input.CanRemoveFilterColumns()) {
		state->output_projection_ids = input.projection_ids;
		state->all_columns.Initialize(context, projection.scan_types);
	}

	state->scan_state->Initialize(std::move(projection.scan_column_ids), &context, input.filters, nullptr);
	state->rg_collection->InitializeScan(QueryContext(context), state->scan_state->table_state,
	                                     state->scan_state->GetColumnIds(), input.filters);
	return std::move(state);
}

static void HeadlessDuckReadFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<HeadlessDuckReadGlobalState>();
	auto &scan_output = gstate.CanRemoveFilterColumns() ? gstate.all_columns : output;
	if (gstate.CanRemoveFilterColumns()) {
		gstate.all_columns.Reset();
	}
	if (!gstate.scan_state->table_state.Scan(scan_output, TableScanType::TABLE_SCAN_COMMITTED_ROWS)) {
		output.SetCardinality(0);
		return;
	}
	if (gstate.CanRemoveFilterColumns()) {
		output.ReferenceColumns(gstate.all_columns, gstate.output_projection_ids);
	}
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
	                       HeadlessDuckReadInitGlobal);
	function.projection_pushdown = true;
	function.pushdown_complex_filter = HeadlessDuckReadPushdownComplexFilter;
	function.filter_pushdown = true;
	function.filter_prune = true;
	return function;
}

} // namespace duckdb
