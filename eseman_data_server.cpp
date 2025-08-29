#include "eseman_data_server.h"
#include <iomanip>
#include <filesystem>

using namespace rapidjson;
using namespace std;

//TODO: dynamically tune the buffer size for larger files to improve I/O performance
#define DEFAULT_READ_BUFFER_SIZE 256*1024 // 256KB

// short name, long name, argument name, default value, description
typedef vector<tuple <string, string, string, string, string> > CMD_OPTIONS;

enum class ESEMAN_MODELS { AGC, KDT, ODKDT };

class ESeManJSONHandler : public BaseReaderHandler<UTF8<>, ESeManJSONHandler> {
public:
    int depth = 0;                // Current object/array nesting depth
    bool inArrayRoot = false;     // True if we are inside the top-level array
    std::string buffer;           // JSON buffer for one object

    std::function<void(const std::string&)> onObject;

    bool StartArray() {
        if (!inArrayRoot && depth == 0) {
            inArrayRoot = true;   // Root array starts
        } else {
            buffer.push_back('[');
        }
        depth++;
        return true;
    }

    bool EndArray(SizeType) {
        depth--;
        if (depth > 0) {
            buffer.push_back(']');
        } else if (inArrayRoot && depth == 0) {
            inArrayRoot = false; // Finished root array
        }
        return true;
    }

    bool StartObject() {
        buffer.push_back('{');
        depth++;
        return true;
    }

    bool EndObject(SizeType) {
        buffer.push_back('}');
        depth--;
        if (depth == 1 && inArrayRoot) {
            // Top-level object just finished
            if (onObject) onObject(buffer);
            buffer.clear();
        }
        return true;
    }

    bool Key(const char* str, SizeType length, bool) {
        if(buffer.size() > 0 && buffer.back() != '{' && buffer.back() != ',') {
            buffer.push_back(',');
        }
        buffer.push_back('"');
        buffer.append(str, length);
        buffer.push_back('"');
        buffer.push_back(':');
        return true;
    }

    bool String(const char* str, SizeType length, bool) {
        if(buffer.size() > 0 && buffer.back() == '"') {
            buffer.push_back(',');
        }
        buffer.push_back('"');
        buffer.append(str, length);
        buffer.push_back('"');
        return true;
    }

    bool Int(int i) { buffer += std::to_string(i); return true; }
    bool Uint(unsigned u) { buffer += std::to_string(u); return true; }
    bool Int64(int64_t i) { buffer += std::to_string(i); return true; }
    bool Uint64(uint64_t u) { buffer += std::to_string(u); return true; }
    bool Double(double d) { buffer += std::to_string(d); return true; }
    bool Bool(bool b) { buffer += (b ? "true" : "false"); return true; }
    bool Null() { buffer += "null"; return true; }
};

void printHelp(const char* progName, const CMD_OPTIONS& options) {
    cout << "Usage: " << progName << " [options]\n\n"
         << "Options:" << endl;
    for (const auto& [opt_s, opt_l, opt_args, def, desc] : options) {
        cout << right << setw(5) << opt_s << ", " << left << setw(10) << opt_l;
        if (!opt_args.empty()) {
            cout << setw(10) << opt_args;
        }
        cout << " " << desc;
        if (!def.empty()) {
            cout << " (default: " << def << ").";
        }
        cout << endl;
    }
    cout << endl;
}

int process_input_arguments(int argc, char* argv[], unordered_map<string, string>& args) {
    CMD_OPTIONS options = {
        {"-h", "--help",    "",         "",     "Show this help message."},
        {"-s", "--start",   "",         "",     "Start the server."},
        {"-b", "--bundle",  "",         "",     "Bundle the input file and store into LMDB."},
        {"-p", "--port",    "<PORT>",   "8080", "Server port"},
        {"-i", "--input",   "<FILE>",   "",     "Specify the event sequence input file location. The input file should be in JSON format."},
        {"-m", "--model",   "<MODEL>",  "KDT",  "Specify the data structure, AGC for agglomerative clustering, KDT for KD-Tree."}
    };
    vector<string> short_names;
    vector<string> long_names;
    for (const auto& opt : options) {
        short_names.push_back(get<0>(opt));
        long_names.push_back(get<1>(opt));
        if (!get<3>(opt).empty()) {
            args[get<1>(opt).substr(2)] = get<3>(opt);
        }
    }

    for (int i = 1; i < argc; i++) {
        string arg = argv[i];

        if (arg.empty() 
            || arg[0] != '-' 
            || (find(short_names.begin(), short_names.end(), arg) == short_names.end()
            && find(long_names.begin(), long_names.end(), arg) == long_names.end())
        ) {
            cerr << "Unknown or malformed option: " << arg << "\n";
            printHelp(argv[0], options);
            return 1;
        }

        if (arg == "-h" || arg == "--help") {
            printHelp(argv[0], options);
            return 0;
        }
        else if (arg == "-s" || arg == "--start") {
            args["start"] = "true";
        }
        else if (arg == "-b" || arg == "--bundle") {
            args["bundle"] = "true";
        }
        else {
            int found_index = -1;
            if (find(short_names.begin(), short_names.end(), arg) != short_names.end()) {
                found_index = distance(short_names.begin(), find(short_names.begin(), short_names.end(), arg));
            } else if (find(long_names.begin(), long_names.end(), arg) != long_names.end()) {
                found_index = distance(long_names.begin(), find(long_names.begin(), long_names.end(), arg));
            }
            if (found_index == -1) {
                cerr << "Unknown option: " << arg << "\n";
                printHelp(argv[0], options);
                return 1;
            }
            if (i + 1 >= argc) {
                cerr << "Option " << arg << " requires an argument.\n";
                printHelp(argv[0], options);
                return 1;
            }
            args[get<1>(options[found_index]).substr(2)] = argv[++i];
        }
    }

    // validity checks
    try {
        if (args.find("input") != args.end() && !args["input"].empty()) {
            if (access(args["input"].c_str(), F_OK) == -1) {
                cerr << "Input file does not exist: " << args["input"] << endl;
                return 1;
            }
        }
        if (args["port"].empty() || stoi(args["port"]) <= 0) {
            cerr << "Invalid port number: " << args["port"] << endl;
            return 1;
        }
        if (args["model"] != "AGC" && args["model"] != "KDT" && args["model"] != "ODKDT") {
            cerr << "Invalid model type: " << args["model"] << ". Supported models are AGC, KDT, ODKDT." << endl;
            return 1;
        }
        if (args.find("bundle") != args.end() && !args["bundle"].empty()) {
            if (args.find("input") == args.end() || args["input"].empty()) {
                cerr << "Input file must be specified when bundling." << endl;
                return 1;
            }
        }
    } catch (...) {
        cerr << "Error in input arguments" << endl;
        return 1;
    }
    
    return 0;
}

int main(int argc, char* argv[]) {
    unordered_map<string, string> args;
    args["port"] = "8080"; // Default port
    AgglomerateClusters *agglomerateClusters = nullptr;
    EseManKDT *esemanKDT = nullptr;

    int c = process_input_arguments(argc, argv, args);
    if (c != 0) {
        return c;
    }

#ifdef _DEBUG
    cout << "Parsed arguments:" << endl;
    for (const auto& [key, value] : args) {
        cout << "  " << key << ": " << value << endl;
    }
#endif

    ESEMAN_MODELS eseman_model = ESEMAN_MODELS::KDT;
    if (args.find("model") != args.end() && args["model"] == string("AGC")) {
        eseman_model = ESEMAN_MODELS::AGC;
    } else if (args.find("model") != args.end() && args["model"] == string("ODKDT")) {
        eseman_model = ESEMAN_MODELS::ODKDT;
    }


    string config_file = "./config.json";
    FILE* fp = fopen(config_file.c_str(), "rb");
    if (!fp) {
        cerr << "Failed to open config file: " << config_file << endl;
        return 1;
    }
    char readBuffer[DEFAULT_READ_BUFFER_SIZE];
    FileReadStream is(fp, readBuffer, sizeof(readBuffer));
    Document doc;
    if (doc.ParseStream(is).HasParseError()) {
        cerr << "Error parsing config file: " << config_file << endl;
        fclose(fp);
        return 1;
    }
    fclose(fp);

    if(doc.IsNull() || !doc.IsObject() || !doc.HasMember("default") || !doc["default"].IsObject()) {
        cerr << "Config file is not a valid JSON object." << endl;
        return 1;
    }

    if(eseman_model == ESEMAN_MODELS::AGC) {
        agglomerateClusters = new AgglomerateClusters();
        agglomerateClusters->horizontal_resolution_divisor = doc["default"].GetObject()["horizontal_pixel_window"].GetInt();
    } else {
        esemanKDT = new EseManKDT();
        esemanKDT->horizontal_resolution_divisor = doc["default"].GetObject()["horizontal_pixel_window"].GetInt();
        esemanKDT->vertical_resolution_divisor = doc["default"].GetObject()["vertical_pixel_window"].GetInt();
        if (eseman_model == ESEMAN_MODELS::ODKDT) {
            esemanKDT->is_vertical_split = true;
        }
        esemanKDT->setDatasetID(doc["default"].GetObject()["database_name"].GetString());
        esemanKDT->node_storage_base_path = doc["default"].GetObject()["database_location"].GetString();
        esemanKDT->lmdb_database_total_size = stoll(doc["default"].GetObject()["LMDB_DATABASE_TOTAL_SIZE"].GetString());
        esemanKDT->ESEMAN_SPLITTING_RULE = doc["default"].GetObject()["ESEMAN_SPLITTING_RULE"].GetString();
        esemanKDT->ESEMAN_TASK_COUNT = doc["default"].GetObject()["ESEMAN_TASK_COUNT"].GetInt();
        esemanKDT->ESEMAN_TASK_ID = doc["default"].GetObject()["ESEMAN_TASK_ID"].GetInt();
#ifdef _DEBUG        
        cout << "values from config file: " << endl;
        cout << "  horizontal_pixel_window: " << esemanKDT->horizontal_resolution_divisor << endl;
        cout << "  vertical_pixel_window: " << esemanKDT->vertical_resolution_divisor << endl;
        cout << "  database_location: " << esemanKDT->node_storage_base_path << endl;
        // cout << "  database_name: " << esemanKDT->getDatasetID() << endl;
        cout << "  lmdb_database_total_size: " << esemanKDT->lmdb_database_total_size << endl;
        cout << "  ESEMAN_SPLITTING_RULE: " << esemanKDT->ESEMAN_SPLITTING_RULE << endl;
        cout << "  ESEMAN_TASK_COUNT: " << esemanKDT->ESEMAN_TASK_COUNT << endl;
        cout << "  ESEMAN_TASK_ID: " << esemanKDT->ESEMAN_TASK_ID << endl;
#endif
    }

    if (args.find("input") != args.end() && !args["input"].empty()) {
        filesystem::path path(args["input"].c_str());
        esemanKDT->setDatasetID(path.stem().c_str());
    }

    // If bundle option is specified, bundle the input file and store into LMDB
    if(args.find("bundle") != args.end()) {
        
        fp = fopen(args["input"].c_str(), "rb");
        if (!fp) {
            cerr << "Failed to open input file: " << args["input"] << endl;
            return 1;
        }
        memset(readBuffer, 0, sizeof(readBuffer));
        FileReadStream is(fp, readBuffer, sizeof(readBuffer));

        Reader reader;
        ESeManJSONHandler handler;

        handler.onObject = [esemanKDT, agglomerateClusters](const std::string& jsonStr) {
            // Remove trailing comma if present
            std::string cleaned = jsonStr;
            if (!cleaned.empty() && cleaned.back() == ',')
                cleaned.pop_back();

            Document d;
            d.Parse(cleaned.c_str());
            if (d.HasParseError()) {
                std::cerr << "Parse error inside object: "
                        << GetParseError_En(d.GetParseError()) << endl
                        << cleaned
                        << endl;
                return;
            }

            if(agglomerateClusters != nullptr) {
                agglomerateClusters->insertDataIntoTree((double)d["enter"]["Timestamp"].GetInt64(),
                                                        (double)d["leave"]["Timestamp"].GetInt64(),
                                                        d["Location"].GetString(),
                                                        d["Primitive"].GetString(),
                                                        d["intervalId"].GetString());
            } else if(esemanKDT != nullptr) {
                esemanKDT->insertDataIntoTree((double)d["enter"]["Timestamp"].GetInt64(),
                                            (double)d["leave"]["Timestamp"].GetInt64(),
                                            d["Location"].GetString(),
                                            d["Primitive"].GetString(),
                                            d["intervalId"].GetString());
            } else {}
        };

        ParseResult ok = reader.Parse(is, handler);
        if (!ok) {
            std::cerr << "Parse error: " 
                    << GetParseError_En(ok.Code()) 
                    << " at offset " << ok.Offset() << "\n";
        }
        fclose(fp);

        if(eseman_model == ESEMAN_MODELS::AGC) {
            agglomerateClusters->buildAllAggClusters();
        } else {
            esemanKDT->buildKDT();
        }
    }

    return 0;
}
