#ifndef ESEMAN_COMMONS_H
#define ESEMAN_COMMONS_H


#include <iostream>
#include <utility>
#include <cstring>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <cstdint>
#include <vector>
#include <cmath>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <functional>
#include <memory>
#include <random>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <climits>
#include <variant>
#include <stack>
#include <lmdb.h> 
// using lmdb because
// - it uses B+ tree
// - memory mapped
// - fast random access based on keys (RocksDB and others uses fast sequentail, slower on random)
// - optimal space usage
// - optimal for write once and read heavy queries
// - only drawback is the database doesnt grow on demand, have to predefined the whole database size

using namespace std;

#ifndef _DEBUG
#define PRINTLOG(x) 
#else
#define PRINTLOG(x) std::cout << x << std::endl
#endif

#define LMDB_DATABASE_TOTAL_SIZE 20L*1024*1024*1024 //20 GB

typedef unordered_map<string, size_t>                 String_to_index;
typedef map<uint64_t, vector<double>>                 LocDict;
typedef unordered_map<string, unordered_set<size_t>>  AttributeList;

// =======================================
// Event related types and inline functions
// =======================================
typedef unordered_map<string, variant<string, double, size_t>> EventDict;
// events are considered ordered by time. odd indexed events are start events and even indexed events are end events.
typedef vector<EventDict>                             EventDictList;


inline double getEventTime(const EventDict& event) {
    try {
        return get<double>(event.at("time"));
    } catch (const out_of_range&) {
        return 0.0;
    } catch (const bad_variant_access&) {
        try {
            return stod(get<string>(event.at("time")));
        } catch (...) {
            return 0.0;
        }
    }
}
inline string getEventPrimitive(const EventDict& event) {
    try {
        return get<string>(event.at("primitive"));
    } catch (...) {
        return "";
    }
}
inline string getEventID(const EventDict& event) {
    try {
        return get<string>(event.at("ID"));
    } catch (...) {
        return "";
    }
}
inline void setEventTime(EventDict& event, double time) {
    event["time"] = time;
}
inline void setEventPrimitive(EventDict& event, const string& primitive) {
    event["primitive"] = primitive;
}
inline void setEventID(EventDict& event, const string& id) {
    event["ID"] = id;
}

class StringIndexMapper {
private:
  const string OUT_OF_RANGE = "Index out of range";
  vector<string> tracks;
  String_to_index track_to_index;
public:
  // Constructor takes an optional custom comparator lambda for ordering
  StringIndexMapper(){}
  ~StringIndexMapper() {
    // PRINTLOG("StringIndexMapper destructor called");
    tracks.clear();
    track_to_index.clear();
  }
  void cleanMemory() {
    tracks.clear();
    track_to_index.clear();
  }
  const string& operator[](size_t idx) const {
    return at(idx);
  }

  // Access string by insertion order
  const string& at(size_t idx) const {
    if (idx >= tracks.size()) {
      return OUT_OF_RANGE;
    }
    return tracks.at(idx);
  }

  // Insert a string if not present, preserving insertion order
  void insert(const string& s) {
    if (track_to_index.find(s) == track_to_index.end()) {
      track_to_index[s] = tracks.size();
      tracks.push_back(s);
    }
  }

  // Returns the index of the string if found, else return size()
  size_t get_track_index(const string& s) {
    if (track_to_index.find(s) == track_to_index.end()) {
      PRINTLOG("Track not found: " << s);
      return tracks.size();
    }
    return track_to_index[s];
  }

  // Number of unique strings
  size_t size() const {
    return tracks.size();
  }

  void order_tracks(const function<bool(const string&, const string&)>& comp) {
    sort(tracks.begin(), tracks.end(), comp);
    // Update track_to_index to reflect new order
    for (size_t i = 0; i < tracks.size(); ++i) {
      track_to_index[tracks[i]] = i;
    }
  }

  void print_tracks() const {
    for (const auto& track : tracks) {
      cout << track << " ";
    }
    cout << endl;
  }
};

typedef unordered_map<string, StringIndexMapper>            AttributeDict;

inline uint64_t getBinSize(int64_t time_begin, int64_t time_end, uint64_t bins){
  return (uint64_t)floor((double)(time_end - time_begin) / (double)bins);
}

inline int getBinNumber(int64_t time_begin, int64_t time_end, uint64_t bins, int64_t ctime) {
  uint64_t bin_size = getBinSize(time_begin, time_end, bins);
  if(ctime < time_begin || ctime > time_end) return -1;
  return (int)floor((double)(ctime - time_begin) / (double)bin_size);
}

inline string doubleToStringZeroPrecision(double value) {
    stringstream ss;
    ss << fixed << setprecision(0) << value;
    return ss.str();
}

#endif // ESEMAN_COMMONS_H