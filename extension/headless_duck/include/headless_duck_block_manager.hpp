//===----------------------------------------------------------------------===//
//                         DuckDB
//
// headless_duck_block_manager.hpp
//
// Minimal append-only BlockManager that backs a .hduck file. Blocks are
// placed at sequential offsets starting at `base_offset`. No free list,
// no rotation, no checksum, no database header.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

class DatabaseInstance;

class HeadlessDuckBlockManager final : public BlockManager {
public:
	enum class Mode { WRITE, READ };

	HeadlessDuckBlockManager(DatabaseInstance &db, BufferManager &buffer_manager, unique_ptr<FileHandle> handle,
	                         Mode mode, idx_t base_offset, idx_t block_alloc_size, idx_t block_header_size);
	~HeadlessDuckBlockManager() override;

	unique_ptr<Block> ConvertBlock(block_id_t block_id, FileBuffer &source_buffer) override;
	unique_ptr<Block> CreateBlock(block_id_t block_id, FileBuffer *source_buffer) override;
	block_id_t GetFreeBlockId() override;
	block_id_t PeekFreeBlockId() override;
	block_id_t GetFreeBlockIdForCheckpoint() override;
	bool IsRootBlock(MetaBlockPointer root) override;
	void MarkBlockAsCheckpointed(block_id_t block_id) override;
	void MarkBlockAsUsed(block_id_t block_id) override;
	void MarkBlockAsModified(block_id_t block_id) override;
	void IncreaseBlockReferenceCount(block_id_t block_id) override;
	idx_t GetMetaBlock() override;
	void Read(QueryContext context, Block &block) override;
	void ReadBlocks(FileBuffer &buffer, block_id_t start_block, idx_t block_count) override;
	void Write(FileBuffer &block, block_id_t block_id) override;
	void WriteHeader(QueryContext context, DatabaseHeader header) override;
	idx_t TotalBlocks() override;
	idx_t FreeBlocks() override;
	bool InMemory() override {
		return false;
	}
	void FileSync() override;

	//! On write, call this after the MetadataManager has persisted its root
	//! so that we can remember it for the file footer. On read, the caller
	//! sets this from the footer before any Read() calls.
	void SetMetaBlockRoot(block_id_t root);
	block_id_t GetMetaBlockRoot() const {
		return meta_block_root;
	}
	void SetBlockCount(idx_t count) {
		block_count = count;
	}

	idx_t BlockOffset(block_id_t id) const;

private:
	DatabaseInstance &db;
	unique_ptr<FileHandle> handle;
	Mode mode;
	idx_t base_offset;

	atomic<block_id_t> next_block_id {0};
	idx_t block_count {0};
	block_id_t meta_block_root {INVALID_BLOCK};
	mutex io_lock;
};

class HeadlessDuckTableIOManager final : public TableIOManager {
public:
	HeadlessDuckTableIOManager(BlockManager &block_manager, idx_t row_group_size)
	    : block_manager(block_manager), row_group_size(row_group_size) {
	}

	BlockManager &GetIndexBlockManager() override {
		return block_manager;
	}
	BlockManager &GetBlockManagerForRowData() override {
		return block_manager;
	}
	MetadataManager &GetMetadataManager() override {
		return block_manager.GetMetadataManager();
	}
	idx_t GetRowGroupSize() const override {
		return row_group_size;
	}

private:
	BlockManager &block_manager;
	idx_t row_group_size;
};

} // namespace duckdb