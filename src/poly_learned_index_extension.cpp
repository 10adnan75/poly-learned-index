#define DUCKDB_EXTENSION_MAIN

#include "POLY-ALEX/src/core/alex.h"
#include "poly_learned_index_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <iostream>
#include <chrono>
#include <numeric>
#include <iomanip>
#include <map>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

int end_point = 0;
std::vector<std::vector<unique_ptr<Base>>> answer;
alex::Alex<int, std::string> poly_learned_index;
map<std::string, pair<std::string, std::string>> index_table_map;

static QualifiedName FetchQualifiedName(ClientContext &context, const string &q_name_str) {
	auto q_name = QualifiedName::Parse(q_name_str);
	if (q_name.schema == INVALID_SCHEMA) {
		q_name.schema = ClientData::Get(context).catalog_search_path->GetDefaultSchema(q_name.catalog);
	}
	return q_name;
}

static void ExistsTable(ClientContext &context, QualifiedName &q_name) {
	unique_ptr<FunctionData> bind_data;
}

template <typename T>
void DisplayElements(T t, const int &width) {
	const char sep = ' ';
	std::cout << std::setw(width) << std::setfill(sep) << t;
}

void DisplayRow(int row_id, vector<std::string> columns) {
	vector<unique_ptr<Base>> &v = answer.at(row_id);
	int width = 10;

	for (string col : columns) {
		DisplayElements(col, width);
	}

	std::cout << "\n";
	for (int i = 0; i < v.size(); i++) {
		if (auto *intData = dynamic_cast<IntData *>(v[i].get())) {
			DisplayElements(intData->value, width);
		}
	}
}

void ExecuteQuery(duckdb::Connection &con, std::string query) {
	unique_ptr<MaterializedQueryResult> result = con.Query(query);
	if (result->HasError()) {
		std::cout << "Query execution failed! " << "\n";
	} else {
		std::cout << "Query execution successful! " << "\n";
	}
}

template <typename K>
void PrintStats() {
	if (typeid(K) == typeid(int)) {
		auto stats = poly_learned_index.get_stats();
		std::cout << "Index Stats \n";
		std::cout << "***************************\n";
		std::cout << "Total no. of Keys : " << stats.num_keys << "\n";
		std::cout << "Total no. of Model Nodes : " << stats.num_model_nodes << "\n";
		std::cout << "Total no. of Data Nodes: " << stats.num_data_nodes << "\n";
	} else {
		std::cout << "Index Stats \n";
		std::cout << "***************************\n";
		std::cout << "Index type not supported! \n";
	}
}

template <typename K, typename P>
void BulkLoad(duckdb::Connection &con, std::string table_name, int column_index) {
}

template <>
void BulkLoad<int, std::string>(duckdb::Connection &con, std::string table_name, int col_index) {
	std::string query = "SELECT * FROM " + table_name + ";";
	unique_ptr<MaterializedQueryResult> result = con.Query(query);
	answer = result->getContents();
	int num_keys = answer.size();

	std::pair<int, std::string> *bulk_load_values = new std::pair<int, std::string>[num_keys];

	int max_key = INT_MIN;

	for (int i = 0; i < answer.size(); i++) {
		int row_id = i;
		auto rrr = answer[i][col_index].get();
		int key_ = dynamic_cast<IntData *>(rrr)->value;

		bulk_load_values[i] = {key_, i};
	}

	auto start_time = std::chrono::high_resolution_clock::now();
	std::sort(bulk_load_values, bulk_load_values + num_keys,
	          [](auto const &a, auto const &b) { return a.first < b.first; });

	poly_learned_index.bulk_load(bulk_load_values, num_keys);
	auto end_time = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = end_time - start_time;
	std::cout << "The total time taken to bulk load is: " << elapsed_seconds.count() << " seconds\n\n\n";
	PrintStats<int>();
}

void CreatePolyLearnedIndexPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
	std::string table_name = parameters.values[0].GetValue<string>();
	std::string col_name = parameters.values[1].GetValue<string>();
	QualifiedName q_name = FetchQualifiedName(context, table_name);
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, q_name.catalog, q_name.schema, q_name.name);
	auto &columnList = table.GetColumns();
	vector<string> col_names = columnList.GetColumnNames();
	vector<LogicalType> col_types = columnList.GetColumnTypes();
	int count = 0;
	int col_index = -1;
	LogicalType col_type;
	int col_i = 0;

	ExistsTable(context, q_name);

	for (col_i = 0; col_i < col_names.size(); col_i++) {
		std::string curr_col_name = col_names[col_i];
		LogicalType curr_col_type = col_types[col_i];
		if (curr_col_name == col_name) {
			col_index = count;
			col_type = curr_col_type;
		}
		count++;
	}

	if (col_index == -1) {
		std::cout << "Column not found! " << "\n";
	} else {
		duckdb::Connection con(*context.db);
		std::string col_type_name = col_type.ToString();

		if (col_type_name == "INTEGER") {
			BulkLoad<int, std::string>(con, table_name, col_index);
			index_table_map.insert({"int", {table_name, col_name}});
		} else {
			std::cout << "Unsupported column type for alex indexing! " << "\n";
		}
	}
}

void FunctionPolyLearnedIndexFind(ClientContext &context, const FunctionParameters &parameters) {
	std::string index_type = parameters.values[0].GetValue<string>();
	std::string key = parameters.values[1].GetValue<string>();

	if (index_type == "int") {
		int key_ = std::stoi(key);
		auto time_start = std::chrono::high_resolution_clock::now();
		auto payload = poly_learned_index.get_payload(key_);
		auto time_end = std::chrono::high_resolution_clock::now();

		if (payload) {
			std::cout << "Payload found! \n";
			pair<std::string, std::string> tab_col = index_table_map["int"];
			std::string table_name = tab_col.first;
			QualifiedName name = FetchQualifiedName(context, table_name);
			auto &table = Catalog::GetEntry<TableCatalogEntry>(context, name.catalog, name.schema, name.name);
			auto &columnList = table.GetColumns();
			vector<string> columnNames = columnList.GetColumnNames();
			// DisplayRow(*payload, columnNames);
			std::chrono::duration<double> elapsed_seconds = time_end - time_start;
			std::cout << "\nTime taken : " << elapsed_seconds.count() << " seconds \n";
		} else {
			std::cout << "Key not found! \n";
		}
		std::cout << "\n";
	}
}

void FunctionLoadCSV(ClientContext &context, const FunctionParameters &parameters) {
	std::string tableName = parameters.values[0].GetValue<string>();
	std::string csvFilePath = parameters.values[1].GetValue<string>();
	bool hasHeader = parameters.values.size() > 2 ? parameters.values[2].GetValue<bool>() : true;

	std::cout << "Loading CSV data from " << csvFilePath << " into table " << tableName << "\n";
	std::cout << "The schema of the table will be {id, name}\n";

	duckdb::Connection con(*context.db);

	std::string CREATE_QUERY = "CREATE TABLE " + tableName + "(id INTEGER, name VARCHAR);";
	ExecuteQuery(con, CREATE_QUERY);

	try {
		std::ifstream file(csvFilePath);
		if (!file.is_open()) {
			std::cerr << "Error: Could not open file: " << csvFilePath << std::endl;
			return;
		}

		std::string line;

		if (hasHeader) {
			std::getline(file, line);
		}

		int count = 0;

		while (std::getline(file, line)) {
			std::stringstream ss(line);
			int id;
			std::string name;
			std::string id_str;

			std::getline(ss, id_str, ',');
			try {
				id = std::stoi(id_str);
			} catch (const std::exception &e) {
				std::cerr << "Error parsing ID from line: " << line << std::endl;
				continue;
			}

			std::getline(ss, name);

			name.erase(0, name.find_first_not_of(" \t"));

			std::string escaped_name = name;

			size_t pos = 0;
			while ((pos = escaped_name.find("'", pos)) != std::string::npos) {
				escaped_name.replace(pos, 1, "''");
				pos += 2;
			}

			std::string query =
			    "INSERT INTO " + tableName + " VALUES (" + std::to_string(id) + ", '" + escaped_name + "')";
			ExecuteQuery(con, query);
			count++;
		}

		std::cout << "CSV data loaded successfully. Inserted " << count << " records." << std::endl;

	} catch (const std::exception &e) {
		std::cerr << "Error loading CSV: " << e.what() << std::endl;
	}
}

void FunctionBatchPolyLearnedIndexFind(ClientContext &context, const FunctionParameters &parameters) {
	std::string index_type = parameters.values[0].GetValue<std::string>();
	std::string query_csv_path = parameters.values[1].GetValue<std::string>();
	std::ifstream infile(query_csv_path);
	std::string line;
	std::getline(infile, line);

	std::vector<std::string> keys;
	while (std::getline(infile, line)) {
		keys.push_back(line);
	}

	int found = 0;
	auto time_start = std::chrono::high_resolution_clock::now();

	for (const auto &key_str : keys) {
		if (index_type == "double") {
			double key = std::stod(key_str);
			auto payload = double_alex_index.get_payload(key);
			if (payload)
				found++;
		} else if (index_type == "bigint") {
			int64_t key = std::stoll(key_str);
			auto payload = big_int_alex_index.get_payload(key);
			if (payload)
				found++;
		} else if (index_type == "int") {
			int key = std::stoi(key_str);
			auto payload = index.get_payload(key);
			if (payload)
				found++;
		} else {
			uint64_t key = std::stoull(key_str);
			auto payload = unsigned_big_int_alex_index.get_payload(key);
			if (payload)
				found++;
		}
	}

	auto time_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = time_end - time_start;

	std::cout << "Batch query complete.\n";
	std::cout << "Found " << found << " out of " << keys.size() << " keys.\n";
	std::cout << "Total time taken: " << elapsed_seconds.count() << " seconds.\n";
	std::cout << "Average time per query: " << (elapsed_seconds.count() / keys.size()) << " seconds.\n";
}

inline void PolyLearnedIndexScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "PolyLearnedIndex " + name.GetString() +
		                                           " Polynomial Regression Extension Installed!");
	});
}

inline void PolyLearnedIndexOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "PolyLearnedIndex " + name.GetString() +
		                                           ", my linked OpenSSL version is " + OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto poly_learned_index_scalar_function =
	    ScalarFunction("poly_learned_index", {LogicalType::VARCHAR}, LogicalType::VARCHAR, PolyLearnedIndexScalarFun);
	ExtensionUtil::RegisterFunction(instance, poly_learned_index_scalar_function);

	// Register another scalar function
	auto poly_learned_index_openssl_version_scalar_function =
	    ScalarFunction("poly_learned_index_openssl_version", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                   PolyLearnedIndexOpenSSLVersionScalarFun);
	ExtensionUtil::RegisterFunction(instance, poly_learned_index_openssl_version_scalar_function);

	// Register a create index function
	auto create_poly_learned_index_function =
	    PragmaFunction::PragmaCall("create_poly_learned_index", CreatePolyLearnedIndexPragmaFunction,
	                               {LogicalType::VARCHAR, LogicalType::VARCHAR}, {});
	ExtensionUtil::RegisterFunction(instance, create_poly_learned_index_function);

	// Register a find index function
	auto find_poly_learned_index_function = PragmaFunction::PragmaCall(
	    "find_poly_learned_index", FunctionPolyLearnedIndexFind, {LogicalType::VARCHAR, LogicalType::VARCHAR}, {});
	ExtensionUtil::RegisterFunction(instance, find_poly_learned_index_function);

    // Register a batch find index function
	auto find_batch_poly_learned_index_function = PragmaFunction::PragmaCall(
	    "find_batch_poly_learned_index", FunctionBatchPolyLearnedIndexFind, {LogicalType::VARCHAR, LogicalType::VARCHAR}, {});
	ExtensionUtil::RegisterFunction(instance, load_csv_data_function);

	// Register a load data function
	auto load_csv_data_function = PragmaFunction::PragmaCall(
	    "load_csv", FunctionLoadCSV, {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN}, {});
	ExtensionUtil::RegisterFunction(instance, load_csv_data_function);
}

void PolyLearnedIndexExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string PolyLearnedIndexExtension::Name() {
	return "poly_learned_index";
}

std::string PolyLearnedIndexExtension::Version() const {
#ifdef EXT_VERSION_POLY_LEARNED_INDEX
	return EXT_VERSION_POLY_LEARNED_INDEX;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void poly_learned_index_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::PolyLearnedIndexExtension>();
}

DUCKDB_EXTENSION_API const char *poly_learned_index_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
