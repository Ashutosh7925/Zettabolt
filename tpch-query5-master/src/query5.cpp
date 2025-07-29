#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <set>
#include <map>

using namespace std;

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], string& r_name, string& start_date, string& end_date, int& num_threads, string& table_path, string& result_path) {
    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        if (arg == "--r_name" && i + 1 < argc) r_name = argv[++i];
        else if (arg == "--start_date" && i + 1 < argc) start_date = argv[++i];
        else if (arg == "--end_date" && i + 1 < argc) end_date = argv[++i];
        else if (arg == "--threads" && i + 1 < argc) num_threads = stoi(argv[++i]);
        else if (arg == "--table_path" && i + 1 < argc) table_path = argv[++i];
        else if (arg == "--result_path" && i + 1 < argc) result_path = argv[++i];
    }
    return !r_name.empty() && !start_date.empty() && !end_date.empty() && num_threads > 0 && !table_path.empty() && !result_path.empty();
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const string& table_path, vector<map<string, string>>& customer_data, vector<map<string, string>>& orders_data, vector<map<string, string>>& lineitem_data, vector<map<string, string>>& supplier_data, vector<map<string, string>>& nation_data, vector<map<string, string>>& region_data) {
    vector<string> customer_cols = {"c_custkey","c_name","c_address","c_nationkey","c_phone","c_acctbal","c_mktsegment","c_comment"};
    vector<string> orders_cols = {"o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk","o_shippriority","o_comment"};
    vector<string> lineitem_cols = {"l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice","l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct","l_shipmode","l_comment"};
    vector<string> supplier_cols = {"s_suppkey","s_name","s_address","s_nationkey","s_phone","s_acctbal","s_comment"};
    vector<string> nation_cols = {"n_nationkey","n_name","n_regionkey","n_comment"};
    vector<string> region_cols = {"r_regionkey","r_name","r_comment"};

    bool ok = true;
    ok &= readTbl(table_path + "/customer.tbl", customer_data, customer_cols);
    ok &= readTbl(table_path + "/orders.tbl", orders_data, orders_cols);
    ok &= readTbl(table_path + "/lineitem.tbl", lineitem_data, lineitem_cols);
    ok &= readTbl(table_path + "/supplier.tbl", supplier_data, supplier_cols);
    ok &= readTbl(table_path + "/nation.tbl", nation_data, nation_cols);
    ok &= readTbl(table_path + "/region.tbl", region_data, region_cols);
    return ok;
}

// Function to execute TPCH Query 5 using multithreading
// bool executeQuery5(const string& r_name, const string& start_date, const string& end_date, int num_threads, const vector<map<string, string>>& customer_data, const vector<map<string, string>>& orders_data, const vector<map<string, string>>& lineitem_data, const vector<map<string, string>>& supplier_data, const vector<map<string, string>>& nation_data, const vector<map<string, string>>& region_data, map<string, double>& results) {
//     results["DUMMY_NATION"] = 12345.67;
//     return true;
// }

bool executeQuery5(const string& r_name, const string& start_date, const string& end_date, int num_threads, 
    const vector<map<string, string>>& customer_data, 
    const vector<map<string, string>>& orders_data, 
    const vector<map<string, string>>& lineitem_data, 
    const vector<map<string, string>>& supplier_data, 
    const vector<map<string, string>>& nation_data, 
    const vector<map<string, string>>& region_data, 
    map<string, double>& results) 
{
    results.clear();

    // 1. Find region keys for r_name
    set<string> region_keys;
    for (const auto& region : region_data) {
        if (region.at("r_name") == r_name)
            region_keys.insert(region.at("r_regionkey"));
    }

    // 2. Find nations in those regions
    map<string, string> nationkey_to_name;
    set<string> nation_keys;
    for (const auto& nation : nation_data) {
        if (region_keys.count(nation.at("n_regionkey"))) {
            nation_keys.insert(nation.at("n_nationkey"));
            nationkey_to_name[nation.at("n_nationkey")] = nation.at("n_name");
        }
    }

    // 3. Find suppliers in those nations
    set<string> supp_keys;
    map<string, string> suppkey_to_nationkey;
    for (const auto& supplier : supplier_data) {
        if (nation_keys.count(supplier.at("s_nationkey"))) {
            supp_keys.insert(supplier.at("s_suppkey"));
            suppkey_to_nationkey[supplier.at("s_suppkey")] = supplier.at("s_nationkey");
        }
    }

    // 4. Find customers in those nations
    set<string> cust_keys;
    map<string, string> custkey_to_nationkey;
    for (const auto& customer : customer_data) {
        if (nation_keys.count(customer.at("c_nationkey"))) {
            cust_keys.insert(customer.at("c_custkey"));
            custkey_to_nationkey[customer.at("c_custkey")] = customer.at("c_nationkey");
        }
    }

    // 5. Find orders by those customers and in date range
    set<string> order_keys;
    map<string, string> orderkey_to_custkey;
    map<string, string> orderkey_to_orderdate;
    for (const auto& order : orders_data) {
        if (cust_keys.count(order.at("o_custkey"))) {
            string odate = order.at("o_orderdate");
            if (odate >= start_date && odate < end_date) {
                order_keys.insert(order.at("o_orderkey"));
                orderkey_to_custkey[order.at("o_orderkey")] = order.at("o_custkey");
                orderkey_to_orderdate[order.at("o_orderkey")] = odate;
            }
        }
    }

    // 6. For each lineitem, join and aggregate
    for (const auto& lineitem : lineitem_data) {
        string orderkey = lineitem.at("l_orderkey");
        string suppkey = lineitem.at("l_suppkey");
        if (order_keys.count(orderkey) && supp_keys.count(suppkey)) {
            string custkey = orderkey_to_custkey[orderkey];
            string nationkey = suppkey_to_nationkey[suppkey];
            string nation_name = nationkey_to_name[nationkey];

            double l_extendedprice = stod(lineitem.at("l_extendedprice"));
            double l_discount = stod(lineitem.at("l_discount"));
            double revenue = l_extendedprice * (1.0 - l_discount);

            results[nation_name] += revenue;
        }
    }

    return true;
}

// Function to output results to the specified path
bool outputResults(const string& result_path, const map<string, double>& results) {
    ofstream out(result_path);
    if (!out.is_open()) return false;
    out << "n_name,revenue\n";
    for (const auto& kv : results) {
        out << kv.first << "," << kv.second << "\n";
    }
    return true;
} 

bool readTbl(const string& filename, vector<map<string, string>>& data, const vector<string>& columns) {
    ifstream file(filename);
    if (!file.is_open()) return false;
    string line;
    while (getline(file, line)) {
        map<string, string> row;
        stringstream ss(line);
        string value;
        int col = 0;
        while (getline(ss, value, '|') && col < columns.size()) {
            row[columns[col++]] = value;
        }
        data.push_back(row);
    }
    return true;
}