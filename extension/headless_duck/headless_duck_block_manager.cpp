#include "headless_duck_block_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/block_allocator.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"

namespace duckdb {

HeadlessDuckBlockManager::HeadlessDuckBlockManager(DatabaseInstance &db, BufferManager &buffer_manager,
                                                   unique_ptr<FileHandle> handle_p, Mode mode_p, idx_t base_offset_p,
                                                   idx_t block_alloc_size_p, idx_t block_header_size_p)
    : BlockManager(buffer_manager, block_alloc_size_p, block_header_size_p), db(db), handle(std::move(handle_p)),
      mode(mode_p), base_offset(base_offset_p) {
}

HeadlessDuckBlockManager::~HeadlessDuckBlockManager() {
	in_destruction = true;
}

idx_t HeadlessDuckBlockManager::BlockOffset(block_id_t id) const {
	return base_offset + static_cast<idx_t>(id) * GetBlockAllocSize();
}

//===----------------------------------------------------------------------===//
// Block construction (shared between read and write)
//===----------------------------------------------------------------------===//

unique_ptr<Block> HeadlessDuckBlockManager::ConvertBlock(block_id_t block_id, FileBuffer &source_buffer) {
	return make_uniq<Block>(source_buffer, block_id, GetBlockHeaderSize());
}

unique_ptr<Block> HeadlessDuckBlockManager::CreateBlock(block_id_t block_id, FileBuffer *source_buffer) {
	if (source_buffer) {
		return ConvertBlock(block_id, *source_buffer);
	}
	return make_uniq<Block>(BlockAllocator::Get(db), block_id, *this);
}

//===----------------------------------------------------------------------===//
// Allocation — monotonically increasing IDs, no free list
//===----------------------------------------------------------------------===//

block_id_t HeadlessDuckBlockManager::GetFreeBlockId() {
	if (mode != Mode::WRITE) {
		throw InternalException("HeadlessDuckBlockManager: GetFreeBlockId in read-only mode");
	}
	auto id = next_block_id++;
	block_count = id + 1;
	return id;
}

block_id_t HeadlessDuckBlockManager::PeekFreeBlockId() {
	return next_block_id.load();
}

block_id_t HeadlessDuckBlockManager::GetFreeBlockIdForCheckpoint() {
	return GetFreeBlockId();
}

//===----------------------------------------------------------------------===//
// Mutability hooks — all no-ops: the file is immutable once written
//===----------------------------------------------------------------------===//

bool HeadlessDuckBlockManager::IsRootBlock(MetaBlockPointer root) {
	return root.block_pointer == static_cast<idx_t>(meta_block_root);
}

void HeadlessDuckBlockManager::MarkBlockAsCheckpointed(block_id_t) {
}

void HeadlessDuckBlockManager::MarkBlockAsUsed(block_id_t) {
}

void HeadlessDuckBlockManager::MarkBlockAsModified(block_id_t) {
	// A modified block would require rewriting the file; we don't support that.
	throw InternalException("HeadlessDuckBlockManager: MarkBlockAsModified is not supported");
}

void HeadlessDuckBlockManager::IncreaseBlockReferenceCount(block_id_t) {
}

//===----------------------------------------------------------------------===//
// Metadata root
//===----------------------------------------------------------------------===//

idx_t HeadlessDuckBlockManager::GetMetaBlock() {
	return static_cast<idx_t>(meta_block_root);
}

void HeadlessDuckBlockManager::SetMetaBlockRoot(block_id_t root) {
	meta_block_root = root;
}

//===----------------------------------------------------------------------===//
// I/O
//===----------------------------------------------------------------------===//

void HeadlessDuckBlockManager::Read(QueryContext context, Block &block) {
	if (mode != Mode::READ) {
		throw InternalException("HeadlessDuckBlockManager: Read in write-only mode");
	}
	if (block.id < 0 || static_cast<idx_t>(block.id) >= block_count) {
		throw IOException("HeadlessDuckBlockManager: block id %lld out of range (%llu blocks)",
		                  (long long)block.id, (unsigned long long)block_count);
	}
	lock_guard<mutex> lock(io_lock);
	block.Read(context, *handle, BlockOffset(block.id));
}

void HeadlessDuckBlockManager::ReadBlocks(FileBuffer &buffer, block_id_t start_block, idx_t count) {
	lock_guard<mutex> lock(io_lock);
	buffer.Read(QueryContext(), *handle, BlockOffset(start_block));
	(void)count; // buffer size already encompasses `count` blocks
}

void HeadlessDuckBlockManager::Write(FileBuffer &buffer, block_id_t block_id) {
	if (mode != Mode::WRITE) {
		throw InternalException("HeadlessDuckBlockManager: Write in read-only mode");
	}
	lock_guard<mutex> lock(io_lock);
	buffer.Write(QueryContext(), *handle, BlockOffset(block_id));
}

void HeadlessDuckBlockManager::WriteHeader(QueryContext, DatabaseHeader) {
	// intentionally empty: we don't use DuckDB's database header; our file
	// format has its own footer which the caller writes after checkpoint.
}

//===----------------------------------------------------------------------===//
// Bookkeeping
//===----------------------------------------------------------------------===//

idx_t HeadlessDuckBlockManager::TotalBlocks() {
	return mode == Mode::WRITE ? next_block_id.load() : block_count;
}

idx_t HeadlessDuckBlockManager::FreeBlocks() {
	return 0;
}

void HeadlessDuckBlockManager::FileSync() {
	lock_guard<mutex> lock(io_lock);
	handle->Sync();
}

} // namespace duckdb