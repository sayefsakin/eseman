#ifndef ESEMAN_DATA_SERVER_H_
#define ESEMAN_DATA_SERVER_H_

#include <unistd.h>
#include <cstdlib>
#include <iomanip>
#include <filesystem>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/config.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "eseman_kdt.h"
#include "agglomerate_clustering.h"

#include "rapidjson/error/en.h"
#include "rapidjson/document.h"
#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filereadstream.h"

using namespace rapidjson;
using namespace std;

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

//TODO: dynamically tune the buffer size for larger files to improve I/O performance
#define DEFAULT_READ_BUFFER_SIZE 256*1024 // 256KB

// short name, long name, argument name, default value, description
typedef vector<tuple <string, string, string, string, string> > CMD_OPTIONS;
// get command name as key, tuple of (param key, bool is string or long long, boolean is required)
typedef unordered_map<string, vector< tuple <string, bool, bool> > > GET_PARAMS;
typedef unordered_map<string, string> STRING_DICT;

enum class ESEMAN_MODELS { AGC, KDT, ODKDT };
ESEMAN_MODELS eseman_model = ESEMAN_MODELS::KDT;

GET_PARAMS get_params = {
    {
        "get-data-in-range", { 
              {"begin", false, false}
            , {"end", false, false}
            , {"tracks", true, false}
            , {"bins", true, false}
            , {"primitive", true, false}
        }
    },
    {
        "get-event-attribute", { 
              {"current-time", false, true}
            , {"current-track", false, true}
        }
    }
};

AgglomerateClusters *agglomerateClusters = nullptr;
EseManKDT *esemanKDT = nullptr;

#endif // ESEMAN_DATA_SERVER_H_