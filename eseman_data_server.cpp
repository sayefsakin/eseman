#include "eseman_data_server.h"

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

int process_input_arguments(int argc, char* argv[], STRING_DICT& args) {
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


Document agcGetAttributeQuery(uint64_t cTime, uint64_t cLocation) {
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    string new_result = agglomerateClusters->findNearestEvent(cTime, cLocation);
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    cout << "AGC," << "ds_attribute," 
        << cTime << "," << cLocation << "," 
        << agglomerateClusters->horizontal_resolution_divisor << ","
        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() 
        << endl;

    Document document;
    document.SetObject();
    Document::AllocatorType& allocator = document.GetAllocator();
    if(new_result.length()>0) {
        Value val(kObjectType);
        val.SetString(new_result.c_str(), static_cast<SizeType>(new_result.length()), allocator);
        document.AddMember("event_id", val, allocator);
    }
    return document;
}

Document esemanGetAttributeQuery(uint64_t cTime, uint64_t cLocation) {
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    string new_result = esemanKDT->findNearestEvent(cTime, cLocation);
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

    if(!new_result.empty())
        cout << "ESEMAN," << "ds_attribute," 
            << cTime << "," << cLocation << ","
            << esemanKDT->horizontal_resolution_divisor << ","
            << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count()
            << endl;

    Document document;
    document.SetObject();
    Document::AllocatorType& allocator = document.GetAllocator();
    if(new_result.length()>0) {
        Value val(kObjectType);
        val.SetString(new_result.c_str(), static_cast<SizeType>(new_result.length()), allocator);
        document.AddMember("event_id", val, allocator);
    }
    return document;
}

Document convertLocDictToDocument(LocDict locDict) {
    Document document;
    document.SetObject();
    Document::AllocatorType& allocator = document.GetAllocator();
    Value tracks(kArrayType);
    for (const auto& myPair : locDict) {
        Value trackObj(kObjectType);

        // "track" field
        string loc_string = to_string(myPair.first);
        Value lval;
        lval.SetString(loc_string.c_str(), static_cast<SizeType>(loc_string.length()), allocator);
        trackObj.AddMember("track", lval, allocator);

        // "utils" field (array)
        Value utilsArr(kArrayType);
        uint64_t bins = myPair.second.size();
        for (uint64_t c_bin = 0; c_bin < bins; c_bin++) {
            string d_string = to_string(myPair.second[c_bin]);
            Value val;
            val.SetString(d_string.c_str(), static_cast<SizeType>(d_string.length()), allocator);
            utilsArr.PushBack(val, allocator);
        }
        trackObj.AddMember("utils", utilsArr, allocator);

        tracks.PushBack(trackObj, allocator);
    }
    document.AddMember("data", tracks, allocator);
    return document;
}

Document binnedAGCSearchQuery(
    int64_t time_begin,
    int64_t time_end,
    vector<string> &locations,
    uint64_t bins, string primitive) {

    if(primitive.length()>0) {
        agglomerateClusters->addPrimitiveFilter(primitive);
    }
    LocDict lResults = agglomerateClusters->binnedRangeQuery(time_begin, time_end, locations, bins);
    Document d = convertLocDictToDocument(lResults);
    lResults.clear();
    return d;
}

void print_available_endpoints(string address, unsigned short port) {
    cout << "ESeMan web server running on http://" << address << ":" << port << endl;
    cout << "Available endpoints (GET only):" << endl;
    for (const auto& [endpoint, params] : get_params) {
        cout << "  GET /" << endpoint << "?";

        bool first = true;
        for (const auto& [param_name, is_string, is_required] : params) {
            if (!first) cout << "&";
            first = false;
            cout << endl << right << setw(30) << param_name << "="
                    << (is_string ? "(string)" : "(integer)") 
                    << (is_required ? "[required]" : "");
        }
        cout << endl;
    }
}

Document binnedESEMANSearchQuery(
    int64_t time_begin,
    int64_t time_end,
    vector<string> &locations,
    uint64_t bins, string primitive) {

    if(primitive.length()>0) {
        esemanKDT->addPrimitiveFilter(primitive);
    }
    tuple<LocDict, int64_t, int64_t> lResults = esemanKDT->binnedRangeQuery(time_begin, time_end, locations, bins);
    Document d = convertLocDictToDocument(get<0>(lResults));

    Document metadata(kObjectType);
    Document::AllocatorType& allocator = d.GetAllocator();

    metadata.AddMember("begin", get<1>(lResults), allocator);
    metadata.AddMember("end", get<2>(lResults), allocator);
    metadata.AddMember("bins", static_cast<uint64_t>(bins), allocator);

    // Value dataKey("data", allocator);
    Value metadataKey("metadata", allocator);

    // // Move the current document (d) under "data"
    // Value dataValue(kObjectType);
    // dataValue.CopyFrom(d, allocator);

    // d.SetObject();
    // d.AddMember(dataKey, dataValue, allocator);
    d.AddMember(metadataKey, metadata, allocator);

    get<0>(lResults).clear();
    return d;
}
class HttpSession : public enable_shared_from_this<HttpSession> {
    beast::tcp_stream stream_;
    beast::flat_buffer buffer_;
    http::request<http::string_body> req_;

public:
    explicit HttpSession(tcp::socket&& socket) : stream_(move(socket)) {}

    void run() {
        do_read();
    }

private:
    STRING_DICT parse_query_params(const string& target) {
        STRING_DICT params;
        
        size_t query_pos = target.find('?');
        if(query_pos == string::npos)
            return params;
        
        string query = target.substr(query_pos + 1);
        istringstream iss(query);
        string pair;
        
        while(getline(iss, pair, '&')) {
            size_t eq_pos = pair.find('=');
            if(eq_pos != string::npos) {
                string key = url_decode(pair.substr(0, eq_pos));
                string value = url_decode(pair.substr(eq_pos + 1));
                params[key] = value;
            }
        }
        
        return params;
    }

    string check_param_validity(const string& path, const STRING_DICT& params) {
        // Remove leading slash if present
        string key = path;
        if (!key.empty() && key[0] == '/') key = key.substr(1);

        // Only check for known endpoints
        if (get_params.find(key) == get_params.end())
            return "Bad request: Unknown endpoint";

        const auto& param_specs = get_params.at(key);
        for (const auto& [param_name, is_string, is_required] : param_specs) {
            auto it = params.find(param_name);
            if (is_required && (it == params.end() || it->second.empty())) {
                return "Missing required parameter: " + param_name;
            }
            if (!is_string && it != params.end() && !it->second.empty()) {
                try {
                    stoll(it->second);
                } catch (...) {
                    return "Parameter '" + param_name + "' must be an integer";
                }
            }
        }
        return "OK";
    }
    
    string url_decode(const string& str) {
        string result;
        for(size_t i = 0; i < str.length(); ++i) {
            if(str[i] == '%' && i + 2 < str.length()) {
                int value;
                istringstream iss(str.substr(i + 1, 2));
                if(iss >> hex >> value) {
                    result += static_cast<char>(value);
                    i += 2;
                } else {
                    result += str[i];
                }
            }
            else if(str[i] == '+') {
                result += ' ';
            } else {
                result += str[i];
            }
        }
        return result;
    }
    
    string get_path_without_query(const string& target) {
        size_t query_pos = target.find('?');
        return query_pos == string::npos ? target : target.substr(0, query_pos);
    }

    void do_read() {
        req_ = {};
        stream_.expires_after(chrono::seconds(30));

        http::async_read(stream_, buffer_, req_,
            [self = shared_from_this()](beast::error_code ec, size_t bytes) {
                if(!ec) self->on_read(ec, bytes);
            });
    }

    void on_read(beast::error_code ec, size_t bytes_transferred) {
        if(ec == http::error::end_of_stream) return do_close();
        if(ec) {
            std::cerr << "Read error: " << ec.message() << std::endl;
            return;
        }
        handle_request();
    }

    string create_error_json(const string& message) {
        Document doc;
        doc.SetObject();
        Document::AllocatorType& allocator = doc.GetAllocator();

        Value error_val;
        error_val.SetString(message.c_str(), message.length(), allocator);
        doc.AddMember("error", error_val, allocator);

        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        doc.Accept(writer);
        
        return buffer.GetString();
    }

    void handle_request() {
        if(req_.method() != http::verb::get) {
            http::response<http::string_body> res{http::status::method_not_allowed, req_.version()};
            res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
            res.set(http::field::content_type, "application/json");

            res.set(http::field::access_control_allow_origin, "*");
            res.set(http::field::access_control_allow_headers, "Content-Type");
            res.set(http::field::access_control_allow_methods, "GET, OPTIONS");

            res.keep_alive(req_.keep_alive());
            res.body() = create_error_json("Only GET requests are supported");
            res.prepare_payload();
            return send_response(move(res));
        }

        string target = string(req_.target());
        string path = get_path_without_query(target);
        STRING_DICT query_params = parse_query_params(target);

        http::response<http::string_body> res{http::status::ok, req_.version()};
        res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(http::field::content_type, "application/json");
        res.set(http::field::access_control_allow_origin, "*");
        res.set(http::field::access_control_allow_headers, "Content-Type");
        res.set(http::field::access_control_allow_methods, "GET, OPTIONS");
        res.keep_alive(req_.keep_alive());

        string params_valid_string = check_param_validity(path, query_params);
        if(params_valid_string != "OK") {
            res.result(http::status::bad_request);
            res.body() = create_error_json(params_valid_string);
        }
        else if (boost::starts_with(target, "/get-data-in-range")) {
            int64_t time_begin = -1;
            if(query_params.find("begin") != query_params.end() && !query_params["begin"].empty())
                time_begin = stoll(query_params["begin"]);
            int64_t time_end = -1;
            if(query_params.find("end") != query_params.end() && !query_params["end"].empty())
                time_end = stoll(query_params["end"]);
            
            vector<string> locationsList;
            if(query_params.find("tracks") != query_params.end() && !query_params["tracks"].empty()) {
                // Split by comma
                istringstream iss(query_params["tracks"]);
                string loc;
                while (getline(iss, loc, ',')) {
                    locationsList.push_back(loc);
                }
            }

            uint64_t bins = 100;
            if(query_params.find("bins") != query_params.end() && !query_params["bins"].empty()) {
                bins = stoul(query_params["bins"]);
            }
            string primitive("");
            if(query_params.find("primitive") != query_params.end() && !query_params["primitive"].empty()) {
                primitive = query_params["primitive"];
            }

            StringBuffer buffer;
            Writer<StringBuffer> writer(buffer);

            if(eseman_model == ESEMAN_MODELS::AGC && agglomerateClusters != nullptr) {
                Document doc = binnedAGCSearchQuery(time_begin, time_end, locationsList, bins, primitive);
                doc.Accept(writer);
                res.body() = buffer.GetString();
            } else if(esemanKDT != nullptr) {
                Document doc = binnedESEMANSearchQuery(time_begin, time_end, locationsList, bins, primitive);
                doc.Accept(writer);
                res.body() = buffer.GetString();
            } else {
                res.result(http::status::internal_server_error);
                res.body() = create_error_json("Data structure not initialized");
            }
        }
        else if (boost::starts_with(target, "/get-event-attribute")) {

            uint64_t cTime = stoll(query_params["current-time"]);
            uint64_t cLocation = stoll(query_params["current-track"]);

            StringBuffer buffer;
            Writer<StringBuffer> writer(buffer);

            if(eseman_model == ESEMAN_MODELS::AGC) {
                Document doc = agcGetAttributeQuery(cTime, cLocation);
                doc.Accept(writer);
                res.body() = buffer.GetString();    
            } else {
                Document doc = esemanGetAttributeQuery(cTime, cLocation);
                doc.Accept(writer);
                res.body() = buffer.GetString();
            }
        }
        else if(target == "/health") {
            // Health check endpoint
            Document doc;
            doc.SetObject();
            Document::AllocatorType& allocator = doc.GetAllocator();
            
            Value status_val;
            status_val.SetString("healthy", allocator);
            doc.AddMember("status", status_val, allocator);
            doc.AddMember("timestamp", time(nullptr), allocator);

            StringBuffer buffer;
            Writer<StringBuffer> writer(buffer);
            doc.Accept(writer);
            
            res.body() = buffer.GetString();
        } else {
            res.result(http::status::not_found);
            res.body() = create_error_json("Endpoint not found");
        }

        res.prepare_payload();
        send_response(move(res));
    }

    void send_response(http::response<http::string_body>&& res) {
        auto sp = make_shared<http::response<http::string_body>>(move(res));
        
        http::async_write(stream_, *sp,
            [self = shared_from_this(), sp](beast::error_code ec, size_t bytes) {
                if(!ec && !sp->need_eof()) {
                    self->do_read();
                } else {
                    self->do_close();
                }
            });
    }

    void do_close() {
        beast::error_code ec;
        stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
    }
};

class Listener : public enable_shared_from_this<Listener> {
    net::io_context& ioc_;
    tcp::acceptor acceptor_;

public:
    Listener(net::io_context& ioc, tcp::endpoint endpoint) : ioc_(ioc), acceptor_(ioc) {
        beast::error_code ec;
        acceptor_.open(endpoint.protocol(), ec);
        if(ec) {
            cerr << "Open error: " << ec.message() << endl;
            return;
        }

        acceptor_.set_option(net::socket_base::reuse_address(true), ec);
        if(ec) {
            cerr << "Set option error: " << ec.message() << endl;
            return;
        }

        acceptor_.bind(endpoint, ec);
        if(ec) {
            cerr << "Bind error: " << ec.message() << endl;
            return;
        }

        acceptor_.listen(net::socket_base::max_listen_connections, ec);
        if(ec) {
            cerr << "Listen error: " << ec.message() << endl;
            return;
        }
    }

    void run() {
        do_accept();
    }

private:
    void do_accept() {
        acceptor_.async_accept(
            [self = shared_from_this()](beast::error_code ec, tcp::socket socket) {
                if(!ec) {
                    make_shared<HttpSession>(move(socket))->run();
                } else {
                    cerr << "Accept error: " << ec.message() << endl;
                }
                
                self->do_accept();
            });
    }
};

void startBoostServer(unsigned short port) {
    string address_str = "127.0.0.1";
    auto const address = net::ip::make_address(address_str);
    net::io_context ioc{max<int>(1, thread::hardware_concurrency())};
    make_shared<Listener>(ioc, tcp::endpoint{address, port})->run();
    print_available_endpoints(address_str, port);

    // Graceful shutdown on SIGINT/SIGTERM
    net::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](beast::error_code const&, int){ 
        cout << "Shutting down the ESeMan server." << endl;
        if(esemanKDT != nullptr) esemanKDT->closeReadOnlyLMDB();
        ioc.stop(); 
    });

    // Run the I/O service on a thread pool
    vector<thread> threads;
    auto n = max(2u, thread::hardware_concurrency());
    threads.reserve(n - 1);
    for (unsigned i = 0; i < n - 1; ++i) threads.emplace_back([&]{ ioc.run(); });
    ioc.run();
    for (auto& t : threads) t.join();
}

int main(int argc, char* argv[]) {
    STRING_DICT args;
    args["port"] = "8080";

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

        handler.onObject = [](const std::string& jsonStr) {
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

    if(args.find("start") != args.end()) {
        if(eseman_model == ESEMAN_MODELS::AGC) {
            startBoostServer(stoi(args["port"]));
        } else {
            esemanKDT->openReadOnlyLMDB();
            if(esemanKDT->reloadNodesFromFile(true)) {    
                startBoostServer(stoi(args["port"]));
            } else {
                cout << "ESEMAN dataset not found on disk" << endl;
            }
        }
    }

    return 0;
}
