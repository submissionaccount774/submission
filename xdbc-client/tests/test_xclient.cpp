#include <iostream>
#include <thread>
#include <numeric>
#include <fstream>
#include <iomanip>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include <boost/program_options.hpp>
#include <nlohmann/json.hpp>
#include "../xdbc/xclient.h"

#include "Tester.h"

static xdbc::SchemaAttribute createSchemaAttribute(std::string name, std::string tpe, int size) {
    xdbc::SchemaAttribute att;
    att.name = std::move(name);
    att.tpe = std::move(tpe);
    att.size = size;
    return att;
}

std::string formatSchema(const std::vector<xdbc::SchemaAttribute> schema) {
    std::stringstream ss;

    // Header line
    ss << std::setw(20) << std::left << "Name"
       << std::setw(15) << std::left << "Type"
       << std::setw(10) << std::left << "Size"
       << '\n';

    for (const auto &tuple: schema) {
        ss << std::setw(20) << std::left << tuple.name
           << std::setw(15) << std::left << tuple.tpe
           << std::setw(10) << std::left << tuple.size
           << '\n';
    }

    return ss.str();
}

using namespace std;
namespace po = boost::program_options;

vector<xdbc::SchemaAttribute> createSchemaFromConfig(const string &configFile) {
    ifstream file(configFile);
    if (!file.is_open()) {
        spdlog::get("XCLIENT")->error("Failed to open schema: {0}", configFile);

    }
    nlohmann::json schemaJson;
    file >> schemaJson;

    vector<xdbc::SchemaAttribute> schema;
    for (const auto &item: schemaJson) {
        schema.emplace_back(xdbc::SchemaAttribute{
                item["name"],
                item["type"],
                item["size"]
        });
    }
    return schema;
}

std::string readJsonFileIntoString(const std::string &filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        spdlog::get("XCLIENT")->error("Failed to open schema: {0}", filePath);
        return "";
    }

    std::stringstream buffer;
    buffer << file.rdbuf();

    return buffer.str();
}


void handleCMDParams(int ac, char *av[], xdbc::RuntimeEnv &env) {
    // Declare the supported options.
    po::options_description desc("Usage: ./test_client [options]\n\nAllowed options");
    desc.add_options()
            ("help,h", "Produce this help message.")
            ("table,e", po::value<string>()->default_value("test_10000000"),
             "Set table: \nDefault:\n  test_10000000")
            ("server-host,a", po::value<string>()->default_value("xdbcserver"),
             "Set server host address: \nDefault:\n  xdbcserver")
            ("intermediate-format,f", po::value<int>()->default_value(1),
             "Set intermediate-format: \nDefault:\n  1 (row)\nOther:\n  2 (col)")
            ("buffer-size,b", po::value<int>()->default_value(64),
             "Set buffer-size of buffers (in KiB).\nDefault: 64")
            ("bufferpool-size,p", po::value<int>()->default_value(4096),
             "Set bufferpool memory size (in KiB).\nDefault: 4096")
            //("tuple-size,t", po::value<int>()->default_value(48), "Set the tuple size.\nDefault: 48")
            ("sleep-time,s", po::value<int>()->default_value(5), "Set a sleep-time in milli seconds.\nDefault: 5ms")
            ("mode,m", po::value<int>()->default_value(1), "1: Analytics, 2: Storage.\nDefault: 1")
            ("net-parallelism,n", po::value<int>()->default_value(1), "Set the network parallelism grade.\nDefault: 1")
            ("write-parallelism,r", po::value<int>()->default_value(1), "Set the read parallelism grade.\nDefault: 1")
            ("decomp-parallelism,d", po::value<int>()->default_value(1),
             "Set the decompression parallelism grade.\nDefault: 1")
            ("transfer-id,tid", po::value<long>()->default_value(0),
             "Set the transfer id.\nDefault: 0");

    po::positional_options_description p;
    p.add("compression-type", 1);

    po::variables_map vm;
    po::store(po::command_line_parser(ac, av).options(desc).positional(p).run(), vm);
    po::notify(vm);

    if (vm.count("help")) {
        cout << desc << "\n";
        exit(0);
    }

    if (vm.count("table")) {
        spdlog::get("XCLIENT")->info("Table: {0}", vm["table"].as<string>());
        env.table = vm["table"].as<string>();
    }
    if (vm.count("intermediate-format")) {
        spdlog::get("XCLIENT")->info("Intermediate format: {0}", vm["intermediate-format"].as<int>());
        env.iformat = vm["intermediate-format"].as<int>();
    }

    if (vm.count("buffer-size")) {
        spdlog::get("XCLIENT")->info("Buffer-size: {0} KiB", vm["buffer-size"].as<int>());
        env.buffer_size = vm["buffer-size"].as<int>();
    }
    if (vm.count("bufferpool-size")) {
        spdlog::get("XCLIENT")->info("Bufferpool size: {0} KiB", vm["bufferpool-size"].as<int>());
        env.buffers_in_bufferpool = vm["bufferpool-size"].as<int>() / vm["buffer-size"].as<int>();
        spdlog::get("XCLIENT")->info("Buffers in Bufferpool: {0}", env.buffers_in_bufferpool);
    }
    /*if (vm.count("tuple-size")) {
        spdlog::get("XCLIENT")->info("Tuple size: {0}", vm["tuple-size"].as<int>());
        env.tuple_size = vm["tuple-size"].as<int>();
    }*/
    if (vm.count("sleep-time")) {
        spdlog::get("XCLIENT")->info("Sleep time: {0} ms", vm["sleep-time"].as<int>());
        env.sleep_time = std::chrono::milliseconds(vm["sleep-time"].as<int>());
    }
    if (vm.count("net-parallelism")) {
        spdlog::get("XCLIENT")->info("Network parallelism: {0}", vm["net-parallelism"].as<int>());
        env.rcv_parallelism = vm["net-parallelism"].as<int>();
    }
    if (vm.count("write-parallelism")) {
        spdlog::get("XCLIENT")->info("Write parallelism: {0}", vm["write-parallelism"].as<int>());
        env.write_parallelism = vm["write-parallelism"].as<int>();
    }
    if (vm.count("decomp-parallelism")) {
        spdlog::get("XCLIENT")->info("Decompression parallelism: {0}", vm["decomp-parallelism"].as<int>());
        env.decomp_parallelism = vm["decomp-parallelism"].as<int>();
    }
    if (vm.count("mode")) {
        spdlog::get("XCLIENT")->info("Mode: {0}", vm["mode"].as<int>());
        env.mode = vm["mode"].as<int>();
    }
    if (vm.count("transfer-id")) {
        spdlog::get("XCLIENT")->info("Transfer id: {0}", vm["transfer-id"].as<long>());
        env.transfer_id = vm["transfer-id"].as<long>();
    }
    if (vm.count("server-host")) {
        spdlog::get("XCLIENT")->info("Server host: {0}", vm["server-host"].as<string>());
        env.server_host = vm["server-host"].as<string>();
    }


    //env.server_host = "xdbcserver";
    env.schemaJSON = "";
    env.server_port = "1234";

    env.tuple_size = 0;
    env.tuples_per_buffer = 0;
    env.monitor.store(false);

}

int main(int argc, char *argv[]) {

    auto console = spdlog::stdout_color_mt("XCLIENT");

    xdbc::RuntimeEnv env;
    handleCMDParams(argc, argv, env);
    env.env_name = "Cpp Client";
    env.startTime = std::chrono::steady_clock::now();

    //create schema
    std::vector<xdbc::SchemaAttribute> schema;

    string schemaFile = "/xdbc-client/tests/schemas/" + env.table + ".json";


    schema = createSchemaFromConfig(schemaFile);
    env.schemaJSON = readJsonFileIntoString(schemaFile);
    env.schema = schema;
    env.tuple_size = std::accumulate(env.schema.begin(), env.schema.end(), 0,
                                     [](int acc, const xdbc::SchemaAttribute &attr) {
                                         return acc + attr.size;
                                     });

    env.tuples_per_buffer = (env.buffer_size * 1024 / env.tuple_size);

    spdlog::get("XCLIENT")->info("Input table: {0} with tuple size {1} and schema:\n{2}",
                                 env.table, env.tuple_size, formatSchema(env.schema));

    Tester tester("Cpp Client", env);

    if (env.mode == 1)
        tester.runAnalytics();
    else if (env.mode == 2)
        tester.runStorage("/dev/shm/output");

    tester.close();

    return 0;
}
