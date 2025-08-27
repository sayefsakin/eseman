#include "eseman_data_server.h"
#include <iomanip>

using namespace std;

// short name, long name, argument name, default value, description
typedef vector<tuple <string, string, string, string, string> > CMD_OPTIONS;

enum class ESEMAN_MODELS { AGC, KDT, ODKDT };

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
        {"-h", "--help",    "",         "",     "Show this help message"},
        {"-s", "--start",   "",         "",     "Start the server"},
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
    int horizontal_resolution_divisor = 1;
    string data_backup_location = "/mnt/d/traveler_dataset_backups";

    int c = process_input_arguments(argc, argv, args);
    if (c != 0) {
        return c;
    }

    cout << "Parsed arguments:" << endl;
    for (const auto& [key, value] : args) {
        cout << "  " << key << ": " << value << endl;
    }
    // if (args.find("input") != args.end()) {
    //     cout << "Input file: " << args["input"] << endl;
    // }

    // ESEMAN_MODELS eseman_model = ESEMAN_MODELS::KDT;
    // if (args.find("model") != args.end() && args["model"] == string("AGC")) {
    //     eseman_model = ESEMAN_MODELS::AGC;
    // } else if (args.find("model") != args.end() && args["model"] == string("ODKDT")) {
    //     eseman_model = ESEMAN_MODELS::ODKDT;
    // }

    // if(eseman_model == ESEMAN_MODELS::AGC) {
    //     agglomerateClusters = new AgglomerateClusters();
    //     agglomerateClusters->horizontal_resolution_divisor = horizontal_resolution_divisor;
    // } else {
    //     esemanKDT = new EseManKDT();
    //     esemanKDT->horizontal_resolution_divisor = horizontal_resolution_divisor;
    //     if (eseman_model == ESEMAN_MODELS::ODKDT) {
    //         esemanKDT->is_vertical_split = true;
    //     }
    //     // esemanKDT->setDatasetID(urlparser.datasetId);
    //     esemanKDT->node_storage_base_path = data_backup_location;
    // }

    return 0;
}
