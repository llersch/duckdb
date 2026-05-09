#define DUCKDB_EXTENSION_MAIN

#include "headless_duck_extension.hpp"
#include "headless_duck_metadata.hpp"
#include "headless_duck_reader.hpp"
#include "headless_duck_writer.hpp"

#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// The toy function we're registering as a smoke test.
//===----------------------------------------------------------------------===//

static void HeadlessDuckHelloFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// Ignore input entirely; produce a constant string.
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto result_data = ConstantVector::GetData<string_t>(result);
	result_data[0] = StringVector::AddString(result, "hello from headless_duck");
}

//===----------------------------------------------------------------------===//
// Extension entry point.
//===----------------------------------------------------------------------===//

static void LoadInternal(ExtensionLoader &loader) {
	ScalarFunction hello_fun("headless_duck_hello", {}, LogicalType::VARCHAR, HeadlessDuckHelloFunction);
	loader.RegisterFunction(hello_fun);

	auto copy_fun = GetHeadlessDuckCopyFunction();
	loader.RegisterFunction(copy_fun);

	auto read_fun = GetHeadlessDuckReadFunction();
	read_fun.name = "read_headlessduck";
	loader.RegisterFunction(read_fun);

	auto stats_fun = GetHeadlessDuckFileStatsFunction();
	loader.RegisterFunction(stats_fun);

	auto storage_info_fun = GetHeadlessDuckStorageInfoFunction();
	loader.RegisterFunction(storage_info_fun);
}

void HeadlessDuckExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string HeadlessDuckExtension::Name() {
	return "headless_duck";
}

std::string HeadlessDuckExtension::Version() const {
#ifdef EXT_VERSION_HEADLESS_DUCK
	return EXT_VERSION_HEADLESS_DUCK;
#else
	return "";
#endif
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
// C entry point — what DuckDB actually calls when loading the .duckdb_extension
// binary dynamically. The macro expands to a specifically-named function that
// DuckDB's loader looks up by symbol name.
//===----------------------------------------------------------------------===//

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(headless_duck, loader) {
	duckdb::LoadInternal(loader);
}
}
