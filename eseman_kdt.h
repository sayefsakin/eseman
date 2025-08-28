#ifndef ESEMAN_KDT_H_
#define ESEMAN_KDT_H_

#include "eseman_commons.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

inline string generate_uuid() {
  return boost::uuids::to_string(boost::uuids::random_generator()());
}

class EsemanNode {
private:
public:
  string        uuid;
  double        start_time;
  double        end_time;
  size_t        start_track;
  size_t        end_track;
  string        left_child;
  string        right_child;
  EsemanNode*   left_node;
  EsemanNode*   right_node;
  AttributeList attribute_lists;

  EsemanNode()
        : uuid(""), start_time(0), end_time(0), start_track(0), end_track(0),
          left_child(""), right_child(""), left_node(nullptr), right_node(nullptr) {}

    EsemanNode(double s_time, double e_time, size_t location)
        : uuid(generate_uuid()), start_time(s_time), end_time(e_time),
          start_track(location), end_track(location),
          left_child(""), right_child(""), left_node(nullptr), right_node(nullptr) {}

  ~EsemanNode() {
    // Don't delete children here - let EseManKDT handle deletion
    for (auto& pair : attribute_lists) {
      pair.second.clear();
    }
    attribute_lists.clear();
  }

  vector<string> getAttributeKeys();
  inline bool hasAttribute(const string& key) const {
    return attribute_lists.find(key) != attribute_lists.end();
  }
  void addAttribute(const string& key, const int attr_index);
  inline bool hasLeftChild() { return !left_child.empty(); }
  inline bool hasRightChild() { return !right_child.empty(); }
  inline bool isLeftChildCached() { return left_node != nullptr; }
  inline bool isRightChildCached() { return right_node != nullptr; }
};

class EseManKDT {
private:
  StringIndexMapper                event_tracks;
  vector<EventDictList>            event_data_values;
  vector<EsemanNode*>              event_data_nodes;
  vector<string>                   eseman_node_uuids;
  AttributeDict                    event_data_attributes;
  string                           return_attribute_key = "";
  bool                             has_return_attribute_key = false;
  bool                             has_filter_query = false;
  EventDictList                    filters;
  int                              max_depth_reached;
  int                              leafs_read;
  int                              nodes_visited;
  string                           dataset_id = "default_dataset";

  MDB_env                         *env;
  MDB_dbi                         dbi;
  MDB_txn                         *txn;

  bool openLMDBENV() {
    int rc = mdb_env_create(&env);
    if (rc) {
        PRINTLOG("mdb_env_create failed, error " << rc);
        return false;
    }

    mdb_env_set_mapsize(env, lmdb_database_total_size < 0 ? LMDB_DATABASE_TOTAL_SIZE : lmdb_database_total_size);

    string dataset_path = node_storage_base_path + "/" + dataset_id + "/eseman.db";
    rc = mdb_env_open(env, dataset_path.c_str(), MDB_NOSUBDIR | MDB_NORDAHEAD, 0664);
    if (rc) {
        PRINTLOG("mdb_env_open failed, error " << rc);
        mdb_env_close(env);
        return false;
    }
    return true;
  }

  bool openWritePermLMDB() {
    if(!openLMDBENV()) return false;
    int rc = mdb_txn_begin(env, NULL, 0, &txn);
    if (rc) {
        PRINTLOG("mdb_txn_begin failed, error " << rc);
        mdb_env_close(env);
        return false;
    }

    rc = mdb_dbi_open(txn, NULL, 0, &dbi);
    if (rc) {
        PRINTLOG("mdb_dbi_open failed, error " << rc);
        mdb_txn_abort(txn);
        mdb_env_close(env);
        return false;
    }
    return true;
  }

  void closeWritePermLMDB() {
    mdb_txn_commit(txn);// committing is important here during the write
    mdb_dbi_close(env, dbi);
    mdb_env_close(env);
  }

  bool checkFilterSatisfied(const EsemanNode* node, const EventDict& filter);
  bool checkFiltersSatisfied(const EsemanNode* node);

  string constructKDTPerTrack(size_t start_index, size_t end_index, size_t track_index);
  string constructTwoDKDT(double start_time, double end_time, size_t start_track, size_t end_track, int depth);
  void printKDTDotRecursive(string uuid, ofstream& dotFile);

  LocDict binnedRangeQueryAllTracks(int64_t time_begin, 
                                            int64_t time_end, 
                                            size_t track_begin, 
                                            size_t track_end, uint64_t bins);
  vector<double> binnedRangeQueryPerTrack(int64_t time_begin, 
                                      int64_t time_end,
                                      size_t track_index,
                                      uint64_t bins,
                                      EsemanNode* replace_node);
  void findClusters(int64_t start_t, int64_t end_t, int64_t bin_size, 
                    EsemanNode* c_node, EsemanNode* replace_node,
                    vector<int64_t> &results, int depth);
  void deleteTree(EsemanNode *node);

  void saveNodeToLMDB(const EsemanNode* node);
  EsemanNode* loadNodeFromLMDB(const string& uuid);
  void deleteFromLMDB(const string& uuid);

  EsemanNode* findNodeInTimeRange(string uuid, double s_time, double e_time, EsemanNode* c_root);
  EsemanNode* checkHotNodes(double start_time, double end_time, size_t track_index);
  void checkNodeAvailability(EsemanNode* c_node, EsemanNode* replace_node);
  void clearDeepNodesFromCache(EsemanNode* c_node);
  void writeNodeUuidAtIndex(string uuid, size_t index);

public:
  int                 horizontal_resolution_divisor = 1;
  int                 vertical_resolution_divisor = 1;
  bool                is_vertical_split = false;
  string              node_storage_base_path = ".";

  uint64_t            lmdb_database_total_size = -1; // in bytes, -1 means use default 1GB
  string              ESEMAN_SPLITTING_RULE = "FAIR";
  int                 ESEMAN_TASK_COUNT = 0;
  int                 ESEMAN_TASK_ID = 0;
  
  EseManKDT() {
      // Constructor logic if needed
  }
  
  ~EseManKDT() {
    for(auto node : event_data_nodes) {
      deleteTree(node);
    }
    event_tracks.cleanMemory();
    filters.clear();
    event_data_values.clear();
    event_data_nodes.clear();
    event_data_attributes.clear();
    eseman_node_uuids.clear();
  }

  bool openReadOnlyLMDB(){
    if(!openLMDBENV()) return false;
    int rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    if (rc) {
        PRINTLOG("mdb_txn_begin failed, error " << rc);
        mdb_env_close(env);
        return false;
    }

    rc = mdb_dbi_open(txn, NULL, 0, &dbi);
    if (rc) {
        PRINTLOG("mdb_dbi_open failed, error " << rc);
        mdb_txn_abort(txn);
        mdb_env_close(env);
        return false;
    }
    return true;
  }
  void closeReadOnlyLMDB() {
    mdb_txn_abort(txn);
    mdb_dbi_close(env, dbi);
    mdb_env_close(env);
  }

  void setDatasetID(const string& ds_id) {
    dataset_id = ds_id;
    if(is_vertical_split) {
      dataset_id = "vertical_split_" + dataset_id;
    }
  }
  void insertDataIntoTree(double start_time, double end_time, string track, string primitive_name, string interval_id);
  void buildKDT();
  void printKDTDotPerTrack(size_t track_index);
  void printKDTDot();

  void cleanNodesFromMemory(bool is_store_existing);
  bool reloadNodesFromFile(bool is_load_attributes);

  void addPrimitiveFilter(string primitive_filter) {
    for (const auto& filter : filters) {
      if(getEventPrimitive(filter) == primitive_filter) return;
    }
    filters.push_back(EventDict{{"primitive", primitive_filter}});
  }
  void addIDFilter(string id_filter) {
    for (const auto& filter : filters) {
      if(getEventID(filter) == id_filter) return;
    }
    filters.push_back(EventDict{{"ID", id_filter}});
  }
  void clearPrimitiveFilters() {
    filters.clear();
  }

  LocDict binnedRangeQuery(int64_t time_begin, int64_t time_end, 
                          vector<string> &locations,
                          uint64_t bins);
  string findNearestEvent(uint64_t cTime, uint64_t cLocation);
};

#endif