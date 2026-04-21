//===----------------------------------------------------------------------===//
//                         DuckDB
//
// headless_duck_format.hpp
//
// On-disk layout constants and structures for the .hduck file format.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

// 8-byte magic, written at the very start of the file and repeated at the
// very end after the footer. Two copies so readers can validate from either
// direction.
static constexpr char HEADLESS_DUCK_MAGIC[8] = {'H', 'D', 'U', 'C', 'K', '1', '\0', '\0'};
static constexpr idx_t HEADLESS_DUCK_MAGIC_SIZE = sizeof(HEADLESS_DUCK_MAGIC);

// Bump when the on-disk layout changes in a way older readers cannot handle.
static constexpr uint32_t HEADLESS_DUCK_FORMAT_VERSION = 2;

// File layout (v1):
//
//   [HeadlessDuckHeader]   — leading magic + version + flags
//   [... payload ...]      — row groups (for now: empty)
//   [HeadlessDuckFooter]   — metadata pointers + trailing magic

struct HeadlessDuckHeader {
	char magic[HEADLESS_DUCK_MAGIC_SIZE];
	uint32_t format_version;
	uint32_t flags; // reserved; must be 0 in v1
};

struct HeadlessDuckFooter {
	uint64_t metadata_offset;
	uint64_t metadata_length;
	uint64_t metadata_checksum;
	uint64_t block_count;
	uint64_t block_alloc_size;
	uint64_t meta_block_root;
	char magic[HEADLESS_DUCK_MAGIC_SIZE];
};

} // namespace duckdb