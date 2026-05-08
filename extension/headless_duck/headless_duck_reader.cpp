#include "headless_duck_reader.hpp"

#include "headless_duck_block_manager.hpp"
#include "headless_duck_format.hpp"

#include "duckdb/common/enums/scan_options.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include <iostream>

#include <cstring>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind data — produced by the bind callback, read by the scan callback
//===----------------------------------------------------------------------===//

struct HeadlessDuckReadBindData : public TableFunctionData {
	string file_path;
	vector<string> column_names;
	vector<LogicalType> sql_types;

	unique_ptr<HeadlessDuckBlockManager> block_manager;
	shared_ptr<HeadlessDuckTableIOManager> table_io;
	shared_ptr<DataTableInfo> data_table_info;
	unique_ptr<RowGroupCollection> rg_collection;
};

//===----------------------------------------------------------------------===//
// Global state — tracks whether we've emitted our single status row yet
//===----------------------------------------------------------------------===//

struct HeadlessDuckReadGlobalState : public GlobalTableFunctionState {
	unique_ptr<TableScanState> scan_state;

	idx_t MaxThreads() const override {
		return 1;
	}
};

//===----------------------------------------------------------------------===//
// Bind: open file, validate envelope, report schema
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> HeadlessDuckReadBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HeadlessDuckReadBindData>();
	result->file_path = input.inputs[0].GetValue<string>();

	auto &fs = FileSystem::GetFileSystem(context);
	auto meta_handle = fs.OpenFile(result->file_path, FileFlags::FILE_FLAGS_READ);
	const idx_t file_size = fs.GetFileSize(*meta_handle);

	const idx_t minimum_size = sizeof(HeadlessDuckHeader) + sizeof(HeadlessDuckFooter);
	if (file_size < minimum_size) {
		throw IOException("headless_duck: file '%s' is %llu bytes, needs at least %llu",
		                  result->file_path, (unsigned long long)file_size, (unsigned long long)minimum_size);
	}

	// --- header ----------------------------------------------------------
	HeadlessDuckHeader header;
	meta_handle->Read(&header, sizeof(header), 0);
	if (std::memcmp(header.magic, HEADLESS_DUCK_MAGIC, HEADLESS_DUCK_MAGIC_SIZE) != 0) {
		throw IOException("headless_duck: bad leading magic in %s", result->file_path);
	}
	if (header.format_version != HEADLESS_DUCK_FORMAT_VERSION) {
		throw IOException("headless_duck: unsupported format_version %u", header.format_version);
	}
	if (header.flags != 0) {
		throw IOException("headless_duck: reserved header flags must be 0");
	}

	// --- footer ----------------------------------------------------------
	HeadlessDuckFooter footer;
	meta_handle->Read(&footer, sizeof(footer), file_size - sizeof(footer));
	if (std::memcmp(footer.magic, HEADLESS_DUCK_MAGIC, HEADLESS_DUCK_MAGIC_SIZE) != 0) {
		throw IOException("headless_duck: bad trailing magic in %s", result->file_path);
	}
	if (footer.metadata_checksum != 0) {
		throw IOException("headless_duck: metadata_checksum is reserved and must be 0");
	}
	const auto footer_start = file_size - sizeof(footer);
	if (footer.metadata_offset > footer_start || footer.metadata_length > footer_start - footer.metadata_offset) {
		throw IOException("headless_duck: footer points outside the file");
	}

	// --- metadata section ------------------------------------------------
	vector<data_t> metadata_buffer(footer.metadata_length);
	meta_handle->Read(metadata_buffer.data(), footer.metadata_length, footer.metadata_offset);
	MemoryStream metadata_stream(metadata_buffer.data(), footer.metadata_length);

	vector<RowGroupPointer> row_group_pointers;
	vector<data_t> mm_bytes;

	{
		BinaryDeserializer deserializer(metadata_stream);
		deserializer.Begin();
		(void)deserializer.ReadProperty<uint32_t>(100, "column_count");
		deserializer.ReadList(101, "columns", [&](Deserializer::List &list, idx_t) {
			list.ReadObject([&](Deserializer &obj) {
				result->column_names.push_back(obj.ReadProperty<string>(200, "name"));
				result->sql_types.push_back(obj.ReadProperty<LogicalType>(201, "type"));
			});
		});
		deserializer.ReadList(103, "row_groups", [&](Deserializer::List &list, idx_t) {
			list.ReadObject([&](Deserializer &obj) {
				row_group_pointers.push_back(RowGroup::Deserialize(obj));
			});
		});
		auto mm_size = deserializer.ReadProperty<uint64_t>(104, "metadata_manager_size");
		mm_bytes.resize(mm_size);
		deserializer.ReadProperty(105, "metadata_manager_bytes", mm_bytes.data(), mm_size);
		deserializer.End();
	}

	// --- construct the read-side object graph ----------------------------
	auto bm_handle = fs.OpenFile(result->file_path, FileFlags::FILE_FLAGS_READ);
	auto &db_instance = *context.db;
	result->block_manager = make_uniq<HeadlessDuckBlockManager>(
	    db_instance, BufferManager::GetBufferManager(db_instance), std::move(bm_handle),
	    HeadlessDuckBlockManager::Mode::READ,
	    /*base_offset*/ sizeof(HeadlessDuckHeader),
	    /*block_alloc_size*/ footer.block_alloc_size, DEFAULT_BLOCK_HEADER_STORAGE_SIZE);
	result->block_manager->SetBlockCount(footer.block_count);

	// restore the MetadataManager's block map
	MemoryStream mm_stream(mm_bytes.data(), mm_bytes.size());
	result->block_manager->GetMetadataManager().Read(mm_stream);

	result->table_io =
	    make_shared_ptr<HeadlessDuckTableIOManager>(*result->block_manager, DEFAULT_ROW_GROUP_SIZE);

	const auto &default_name = DatabaseManager::GetDefaultDatabase(context);
	auto attached = DatabaseManager::Get(db_instance).GetDatabase(context, default_name);
	if (!attached) {
		throw IOException("headless_duck: no default attached database");
	}
	result->data_table_info =
	    make_shared_ptr<DataTableInfo>(*attached, result->table_io, "hduck", "read");

	// rebuild PersistentCollectionData from stored pointers
	PersistentCollectionData persistent_collection;
	for (auto &rgp : row_group_pointers) {
		PersistentRowGroupData prg;
		prg.types = result->sql_types;
		prg.start = rgp.row_start;
		prg.count = rgp.tuple_count;
		for (idx_t c = 0; c < rgp.data_pointers.size(); c++) {
			auto &ptr = rgp.data_pointers[c];
			const LogicalType &col_type = result->sql_types[c];
			MetadataReader mr(result->block_manager->GetMetadataManager(), ptr);
			BinaryDeserializer bd(mr);
			bd.Set<const LogicalType &>(col_type);
			bd.Begin();
			prg.column_data.push_back(PersistentColumnData::Deserialize(bd));
			bd.End();
			bd.Unset<LogicalType>();
		}
		persistent_collection.row_group_data.push_back(std::move(prg));
	}

	result->rg_collection = make_uniq<RowGroupCollection>(result->data_table_info, *result->block_manager,
	                                                      result->sql_types, /*row_start*/ 0, /*total_rows*/ 0,
	                                                      DEFAULT_ROW_GROUP_SIZE);
	result->rg_collection->Initialize(persistent_collection);

	return_types = result->sql_types;
	names = result->column_names;
	return std::move(result);
}

//===----------------------------------------------------------------------===//
// Init / scan
//===----------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> HeadlessDuckReadInitGlobal(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<HeadlessDuckReadBindData>();
	auto state = make_uniq<HeadlessDuckReadGlobalState>();
	state->scan_state = make_uniq<TableScanState>();

	vector<StorageIndex> column_ids;
	for (idx_t i = 0; i < bind.sql_types.size(); i++) {
		column_ids.emplace_back(i);
	}
	state->scan_state->Initialize(column_ids, &context, nullptr, nullptr);
	bind.rg_collection->InitializeScan(QueryContext(context), state->scan_state->table_state, column_ids,
	                                   nullptr);
	return std::move(state);
}

static void HeadlessDuckReadFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<HeadlessDuckReadGlobalState>();
	if (!gstate.scan_state->table_state.Scan(output, TableScanType::TABLE_SCAN_COMMITTED_ROWS)) {
		output.SetCardinality(0);
	}
}

//===----------------------------------------------------------------------===//
// Factory
//===----------------------------------------------------------------------===//

TableFunction GetHeadlessDuckReadFunction() {
	TableFunction function("read_headlessduck", {LogicalType::VARCHAR}, HeadlessDuckReadFunction,
	                       HeadlessDuckReadBind, HeadlessDuckReadInitGlobal);
	return function;
}

} // namespace duckdb
