#ifndef ESEMAN_DATA_SERVER_H_
#define ESEMAN_DATA_SERVER_H_

#include <unistd.h>
#include <cstdlib>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/config.hpp>

#include "eseman_kdt.h"
#include "agglomerate_clustering.h"

#include "rapidjson/error/en.h"
#include "rapidjson/document.h"
#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filereadstream.h"

#endif // ESEMAN_DATA_SERVER_H_