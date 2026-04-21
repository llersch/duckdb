#include "headless_duck_writer.hpp"

#include "headless_duck_block_manager.hpp"
#include "headless_duck_format.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/transaction/transaction_data.hpp"

#include <iostream>

#include <cstring>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Per-function state
//===----------------------------------------------------------------------===//

// Returned by copy_to_bind. Holds anything fixed for the duration of the COPY.
// Right now there's nothing to hold; we still need to return an object because
// bind_data is passed by reference to the other callbacks.
struct HeadlessDuckWriteBindData : public TableFunctionData {
	vector<string> column_names;
	vector<LogicalType> sql_types;
};

// Returned by copy_to_initialize_global. Owns resources shared across threads.
// The file handle and a running byte counter live here.
struct HeadlessDuckWriteGlobalState : public GlobalFunctionData {
	unique_ptr<FileHandle> file_handle;
	idx_t bytes_written = 0;
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
	mutex collection_lock;

	// New — block-managed storage path
	unique_ptr<HeadlessDuckBlockManager> block_manager;
	shared_ptr<HeadlessDuckTableIOManager> table_io;
	shared_ptr<DataTableInfo> data_table_info;
	unique_ptr<RowGroupCollection> rg_collection;
};

// Returned by copy_to_initialize_local. Per-thread state. We don't need any
// yet; we still have to return a non-null object.
struct HeadlessDuckWriteLocalState : public LocalFunctionData {};

//===----------------------------------------------------------------------===//
// Callbacks
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> HeadlessDuckWriteBind(ClientContext &context, CopyFunctionBindInput &input,
                                                      const vector<string> &names,
                                                      const vector<LogicalType> &sql_types) {
	auto bind = make_uniq<HeadlessDuckWriteBindData>();
	bind->column_names = names;
	bind->sql_types = sql_types;
	return std::move(bind);
}

static unique_ptr<LocalFunctionData> HeadlessDuckWriteInitializeLocal(ExecutionContext &context,
                                                                      FunctionData &bind_data) {
	return make_uniq<HeadlessDuckWriteLocalState>();
}

static unique_ptr<GlobalFunctionData>
HeadlessDuckWriteInitializeGlobal(ClientContext &context, FunctionData &bind_data, const string &file_path) {
	auto &bind = bind_data.Cast<HeadlessDuckWriteBindData>();
	auto &fs = FileSystem::GetFileSystem(context);
	auto &db_instance = *context.db;

	auto state = make_uniq<HeadlessDuckWriteGlobalState>();
	state->file_handle =
	    fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);

	// leading header
	HeadlessDuckHeader header;
	std::memcpy(header.magic, HEADLESS_DUCK_MAGIC, HEADLESS_DUCK_MAGIC_SIZE);
	header.format_version = HEADLESS_DUCK_FORMAT_VERSION;
	header.flags = 0;
	state->file_handle->Write(&header, sizeof(header));
	state->bytes_written += sizeof(header);

	// existing ColumnDataCollection (still used as source of truth for now)
	state->collection = make_uniq<ColumnDataCollection>(Allocator::Get(context), bind.sql_types);
	state->collection->InitializeAppend(state->append_state);

	// block manager — opens a *second* handle to the same file; its writes
	// land at offsets [sizeof(header), sizeof(header) + block_count * alloc_size)
	auto bm_handle =
	    fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ);
	state->block_manager = make_uniq<HeadlessDuckBlockManager>(
	    db_instance, BufferManager::GetBufferManager(db_instance), std::move(bm_handle),
	    HeadlessDuckBlockManager::Mode::WRITE,
	    /*base_offset*/ sizeof(HeadlessDuckHeader),
	    DEFAULT_BLOCK_ALLOC_SIZE, DEFAULT_BLOCK_HEADER_STORAGE_SIZE);
	state->table_io = make_shared_ptr<HeadlessDuckTableIOManager>(*state->block_manager,
	                                                              DEFAULT_ROW_GROUP_SIZE);

	const auto &default_name = DatabaseManager::GetDefaultDatabase(context);
	auto attached = DatabaseManager::Get(db_instance).GetDatabase(context, default_name);
	if (!attached) {
		throw IOException("headless_duck: no default attached database");
	}
	state->data_table_info = make_shared_ptr<DataTableInfo>(*attached, state->table_io, "hduck", "test");
	state->rg_collection = make_uniq<RowGroupCollection>(
	    state->data_table_info, *state->block_manager, bind.sql_types,
	    /*row_start*/ 0, /*total_rows*/ 0, DEFAULT_ROW_GROUP_SIZE);
	state->rg_collection->InitializeEmpty();

	return std::move(state);
}

static void HeadlessDuckWriteSink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                  LocalFunctionData &lstate, DataChunk &input) {
	auto &global = gstate.Cast<HeadlessDuckWriteGlobalState>();
	lock_guard<mutex> lock(global.collection_lock);
	global.collection->Append(global.append_state, input);
}

static void HeadlessDuckWriteCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                     LocalFunctionData &lstate) {
	// no-op — data is written directly to global in Sink
}

static void HeadlessDuckWriteFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	auto &state = gstate.Cast<HeadlessDuckWriteGlobalState>();
	auto &bind = bind_data.Cast<HeadlessDuckWriteBindData>();

	auto &db_instance = *context.db;
	const auto &default_name = DatabaseManager::GetDefaultDatabase(context);
	auto attached = DatabaseManager::Get(db_instance).GetDatabase(context, default_name);
	D_ASSERT(attached);

	// --- Append all accumulated chunks into the RowGroupCollection ---
	TableAppendState append_state;
	state.rg_collection->InitializeAppend(append_state);
	for (auto &chunk : state.collection->Chunks()) {
		state.rg_collection->Append(chunk, append_state);
	}
	state.rg_collection->FinalizeAppend(TransactionData::Committed(), append_state);

	// --- Write each row group's segments via our block manager ---
	PartialBlockManager partial_bm(QueryContext(context), *state.block_manager,
	                               PartialBlockType::FULL_CHECKPOINT);
	vector<CompressionType> compression_types(bind.sql_types.size(), CompressionType::COMPRESSION_AUTO);
	RowGroupWriteInfo write_info(partial_bm, compression_types);

	vector<RowGroupWriteData> all_write_data;
	const idx_t rg_count = state.rg_collection->GetRowGroupCount();
	all_write_data.reserve(rg_count);
	for (idx_t i = 0; i < rg_count; i++) {
		auto rg = state.rg_collection->GetRowGroup(static_cast<int64_t>(i));
		if (!rg) {
			continue;
		}
		all_write_data.emplace_back(rg->WriteToDisk(write_info));
	}
	partial_bm.FlushPartialBlocks();

	// --- Serialize per-column metadata → RowGroupPointers ---
	MetadataWriter meta_writer(state.block_manager->GetMetadataManager());
	SerializationOptions ser_options(*attached);

	vector<RowGroupPointer> row_group_pointers;
	idx_t row_start = 0;
	for (auto &write_data : all_write_data) {
		RowGroupPointer rgp;
		rgp.row_start = row_start;
		rgp.tuple_count = write_data.result_row_group->count.load();
		row_start += rgp.tuple_count;
		for (auto &col_state : write_data.states) {
			rgp.data_pointers.push_back(meta_writer.GetMetaBlockPointer());
			auto persistent = col_state->ToPersistentData();
			BinarySerializer s(meta_writer, ser_options);
			s.Begin();
			persistent.Serialize(s);
			s.End();
		}
		rgp.has_metadata_blocks = true;
		row_group_pointers.push_back(std::move(rgp));
	}

	// Record the root meta block; flush metadata blocks to the block manager.
	auto meta_root = meta_writer.GetMetaBlockPointer();
	(void)meta_root; // not used directly — the meta chain root is the *first* block written
	meta_writer.Flush();
	state.block_manager->GetMetadataManager().Flush();
	state.block_manager->FileSync();

	const idx_t block_count = state.block_manager->TotalBlocks();
	const idx_t block_alloc_size = state.block_manager->GetBlockAllocSize();
	const idx_t blocks_end = sizeof(HeadlessDuckHeader) + block_count * block_alloc_size;

	// --- Build the metadata section (schema + CDC data + row_group_pointers) ---
	MemoryStream metadata_stream;
	{
		BinarySerializer serializer(metadata_stream);
		serializer.Begin();
		serializer.WriteProperty(100, "column_count", static_cast<uint32_t>(bind.column_names.size()));
		serializer.WriteList(101, "columns", bind.column_names.size(),
		                     [&](Serializer::List &list, idx_t i) {
			                     list.WriteObject([&](Serializer &obj) {
				                     obj.WriteProperty(200, "name", bind.column_names[i]);
				                     obj.WriteProperty(201, "type", bind.sql_types[i]);
			                     });
		                     });
		// new — row group pointers, serialized as a list
		serializer.WriteList(103, "row_groups", row_group_pointers.size(),
		                     [&](Serializer::List &list, idx_t i) {
			                     list.WriteObject([&](Serializer &obj) {
				                     RowGroup::Serialize(row_group_pointers[i], obj);
			                     });
		                     });
		// NEW: serialize the MetadataManager's block map as raw bytes so the
		// reader can reconstruct it before reading PersistentColumnData.
		MemoryStream mm_stream;
		state.block_manager->GetMetadataManager().Write(mm_stream);
		serializer.WriteProperty(104, "metadata_manager_size",
		                         static_cast<uint64_t>(mm_stream.GetPosition()));
		serializer.WriteProperty(105, "metadata_manager_bytes",
		                         const_data_ptr_cast(mm_stream.GetData()), mm_stream.GetPosition());
		serializer.End();
	}

	const idx_t metadata_offset = blocks_end;
	const idx_t metadata_length = metadata_stream.GetPosition();
	state.file_handle->Write(QueryContext(context), metadata_stream.GetData(), metadata_length, metadata_offset);
	state.bytes_written = metadata_offset + metadata_length;

	// --- Footer ---
	HeadlessDuckFooter footer;
	footer.metadata_offset = metadata_offset;
	footer.metadata_length = metadata_length;
	footer.metadata_checksum = 0;
	footer.block_count = block_count;
	footer.block_alloc_size = block_alloc_size;
	// The meta-block tree root is block_id 1 in our file (block 0 is the first
	// data block). For a single-row-group file this is correct. We'll make
	// this precise in a later step.
	footer.meta_block_root = (block_count > 1) ? 1 : 0;
	std::memcpy(footer.magic, HEADLESS_DUCK_MAGIC, HEADLESS_DUCK_MAGIC_SIZE);
	state.file_handle->Write(QueryContext(context), &footer, sizeof(footer), state.bytes_written);
	state.bytes_written += sizeof(footer);

	state.file_handle->Sync();
	state.file_handle->Close();
}

//===----------------------------------------------------------------------===//
// Factory
//===----------------------------------------------------------------------===//

CopyFunction GetHeadlessDuckCopyFunction() {
	CopyFunction function("headless_duck");
	function.copy_to_bind = HeadlessDuckWriteBind;
	function.copy_to_initialize_local = HeadlessDuckWriteInitializeLocal;
	function.copy_to_initialize_global = HeadlessDuckWriteInitializeGlobal;
	function.copy_to_sink = HeadlessDuckWriteSink;
	function.copy_to_combine = HeadlessDuckWriteCombine;
	function.copy_to_finalize = HeadlessDuckWriteFinalize;
	function.extension = "hduck";
	return function;
}

} // namespace duckdb