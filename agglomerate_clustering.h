#ifndef AGGLOMERATE_CLUSTERING_H_
#define AGGLOMERATE_CLUSTERING_H_

#include "eseman_commons.h"

inline int changeMerge(int i, int N){
  if (i < 0) {
    return (i+1) * -1;
  } else {
    return (i - 1) + N;
  }
}

// this is a one dimensional agglomerate clustering for event sequences.
// Start and end events will be stored in a vector, even number index will hold the start event and odd number index will hold the end event.
class EventAgglomerateClustering {
  private:
    vector<EventDict> data;
    int* merge = NULL;
    double* height = NULL;
    int* node_size = NULL;
    double* start_events = NULL;
    double* end_events = NULL;
    int npoints = 0;
    vector<AttributeList> attribute_lists;
    string return_attribute_key = "";

    int searchEvent(int64_t tb, int begin_index);
    void findClusters(int64_t start_t, int64_t end_t, int64_t bin_size, int c_node, vector<int64_t> &results);
    bool checkFilterSatisfied(const size_t node_id, const EventDict& filter);
    bool checkFiltersSatisfied(const size_t node_id);

  public:
    EventAgglomerateClustering() {
      // Initialization logic for AgglomerateClustering
    }

    ~EventAgglomerateClustering() {
      // Memory cleanup logic for AgglomerateClustering
      data.clear();
      delete[] merge;
      delete[] height;
      delete[] node_size;
      delete[] start_events;
      delete[] end_events;
      attribute_lists.clear();
    }

    void insertDataIntoTree(double start_time, double end_time, string primitive_name, string interval_id);
    void buildAggCluster(AttributeDict& event_data_attributes);
    vector<double> binnedRangeQuery(int64_t time_begin, int64_t time_end, uint64_t bins, int hrd);
    int64_t findNearestEvent(uint64_t cTime);

    void getDataSize() {
      PRINTLOG("Data size: " << data.size());
    }
    string track;
    EventDictList filters;

    vector<string> getAttributeKeysAtIndex(size_t index);
    bool hasAttributeAtIndex(size_t index, const string& key);
    void addAttributeAtIndex(size_t index, const string& key, const int attr_index);
};

class AgglomerateClusters {
  private:
    map<string, EventAgglomerateClustering> agglomerate_clusters;
    AttributeDict                           event_data_attributes;

  public:
    int horizontal_resolution_divisor = 1;
    EventDictList filters;
    AgglomerateClusters() {
      // Initialization logic for AgglomerateClusters
    }
    ~AgglomerateClusters() {
      agglomerate_clusters.clear();
      event_data_attributes.clear();
      // Memory cleanup logic for AgglomerateClusters
    }
    void insertDataIntoTree(double start_time, double end_time, string track, string primitive_name, string interval_id);
    void buildAllAggClusters();
    LocDict binnedRangeQuery(int64_t time_begin, 
      int64_t time_end, 
      vector<string> &locations,
      uint64_t bins);
    string findNearestEvent(uint64_t cTime, uint64_t cLocation);
    
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
};
#endif