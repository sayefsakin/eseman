#include <chrono>

#include "agglomerate_clustering.h"
#include "fstream"

#include "hclust-cpp/fastcluster.h"

inline double distance_event(const double& s11, const double& s12, const double& s21, const double& s22) {
    if (s12 < s21)
      return s21 - s12;
    return s11 - s22;
}

// Attribute functions
vector<string> EventAgglomerateClustering::getAttributeKeysAtIndex(size_t index) {
    vector<string> keys;
    for (const auto& pair : attribute_lists[index]) {
        keys.push_back(pair.first);
    }
    return keys;
}

bool EventAgglomerateClustering::hasAttributeAtIndex(size_t index, const string& key) {
    return attribute_lists[index].find(key) != attribute_lists[index].end();
}

void EventAgglomerateClustering::addAttributeAtIndex(size_t index, const string& key, const int attr_index) {
    if(!hasAttributeAtIndex(index, key)) {
      attribute_lists[index][key] = unordered_set<size_t>();
    }
    attribute_lists[index][key].insert(attr_index);
}


void EventAgglomerateClustering::insertDataIntoTree(double start_time, double end_time, string primitive_name, string interval_id) {
    if (start_time > end_time) {
        PRINTLOG("Invalid time range: start_time > end_time");
        return;
    }
    data.push_back(EventDict{{"time",start_time},{"primitive", primitive_name},{"ID", interval_id}});
    data.push_back(EventDict{{"time",end_time},{"primitive", primitive_name},{"ID", interval_id}});
}

void EventAgglomerateClustering::buildAggCluster(AttributeDict& event_data_attributes) {
    // Implement the agglomerate clustering algorithm here
    // This is a placeholder for the actual clustering logic
    PRINTLOG("Building Agglomerate Clusters...");

    int i,j,k;
    const int opt_method = HCLUST_METHOD_SINGLE;

    npoints = data.size() / 2;
    // computation of condensed distance matrix
    double* distmat = new double[(npoints*(npoints-1))/2];
    k = 0;
    
    for (i=0; i<npoints; i++) {
        for (j=i+1; j<npoints; j++) {
        distmat[k] = distance_event(getEventTime(data[i*2]), 
                                    getEventTime(data[(i*2)+1]), 
                                    getEventTime(data[j*2]),
                                    getEventTime(data[(j*2)+1]));
        k++;
        }
    }

    // clustering call
    merge = new int[2*(npoints-1)];
    height = new double[npoints-1];
    node_size = new int[2*(npoints-1)];
    start_events = new double[2*npoints];
    end_events = new double[2*npoints];
    attribute_lists.resize(2*npoints, AttributeList());
    hclust_fast(npoints, distmat, opt_method, merge, height, node_size);


    int left_node_index, right_node_index, root_node_index;
    for (i=0; i<npoints-1; i++) {
      left_node_index = changeMerge(merge[i], npoints);
      if(left_node_index < npoints) {
        start_events[left_node_index] = getEventTime(data[left_node_index*2]);
        end_events[left_node_index] = getEventTime(data[(left_node_index*2)+1]);

        for (const auto& [key, indexes] : data[left_node_index*2]) {
          if (key == "time") continue;
          size_t attr_index = event_data_attributes[key].get_track_index(get<string>(indexes));
          addAttributeAtIndex(left_node_index, key, attr_index);
        }
      }

      right_node_index = changeMerge(merge[i+npoints-1], npoints);
      if(right_node_index < npoints) {
        start_events[right_node_index] = getEventTime(data[right_node_index*2]);
        end_events[right_node_index] = getEventTime(data[(right_node_index*2)+1]);

        for (const auto& [key, indexes] : data[right_node_index*2]) {
          if (key == "time") continue;
          size_t attr_index = event_data_attributes[key].get_track_index(get<string>(indexes));
          addAttributeAtIndex(right_node_index, key, attr_index);
        }
      }

      if(end_events[left_node_index] > start_events[right_node_index]) {
        swap(left_node_index, right_node_index);
      }
      
      root_node_index = i + npoints;
      start_events[root_node_index] = start_events[left_node_index];
      end_events[root_node_index] = end_events[right_node_index];

      for (const auto& [key, indexes] : attribute_lists[left_node_index]) {
        for (size_t index : indexes)
          addAttributeAtIndex(root_node_index, key, index);
      }
      for (const auto& [key, indexes] : attribute_lists[right_node_index]) {
        for (size_t index : indexes)
          addAttributeAtIndex(root_node_index, key, index);
      }
    }
    
    delete[] distmat;
    PRINTLOG("Building Agglomerate Clusters Done!");
}

int EventAgglomerateClustering::searchEvent(int64_t tb, int begin_index) {
  int left = begin_index, right = npoints-1;
  int s_begin = 0;
  while(left < right) {
    int mid = (left + right) / 2;
    if(getEventTime(data[(mid*2)+1]) < tb) {
      left = mid + 1;
    } else if(getEventTime(data[mid*2]) > tb) {
      right = mid - 1;
    } else {
      s_begin = mid;
      break;
    }
  }
  if (left >= right) {
    s_begin = left+1;
  }
  if(s_begin > npoints-1) s_begin = npoints-1;
  if(s_begin < 0) s_begin = 0;
  return s_begin;
}

// search logic
// if bin size is less than cluster length, then go down
// else return the start end point of the current cluster
// dfs on the start and end time query
void EventAgglomerateClustering::findClusters(int64_t start_t, int64_t end_t, int64_t bin_size, int c_node, vector<int64_t> &results) {
  int root_node = c_node + npoints;

  if(!checkFiltersSatisfied(root_node))return;

  int64_t start_time = (int64_t)start_events[root_node];
  int64_t end_time = (int64_t)end_events[root_node];
  if(start_time >= end_t || end_time <= start_t) return;
  if(bin_size >= (end_time - start_time + 1)) {
    if(return_attribute_key != "") {
      if(!hasAttributeAtIndex(root_node, return_attribute_key)) {
        PRINTLOG("Attribute not found at index: " << root_node << ", key: " << return_attribute_key);
        return;
      }
      results.push_back((int64_t)(*attribute_lists[root_node][return_attribute_key].begin()));
    } else {
      results.push_back(start_time);
      results.push_back(end_time);
    }
    PRINTLOG("Cluster: " << root_node << ", Start: " << start_time << ", End: " << end_time);
    return;
  }
  if(root_node < npoints) {
    if(start_time < start_t) {
      start_time = start_t;
    }
    if(end_time > end_t) {
      end_time = end_t;
    }
    if(return_attribute_key != "") {
      if(!hasAttributeAtIndex(root_node, return_attribute_key)) {
        PRINTLOG("Attribute not found at index: " << root_node << ", key: " << return_attribute_key);
        return;
      }
      results.push_back((int64_t)(*attribute_lists[root_node][return_attribute_key].begin()));
    } else {
      results.push_back(start_time);
      results.push_back(end_time);
    }
    PRINTLOG("Cluster-Leaf: " << root_node << ", Start: " << start_time << ", End: " << end_time);
    return;
  }
  // this is a compound node
  int left_node_index = changeMerge(merge[c_node], npoints);
  int right_node_index = changeMerge(merge[c_node+npoints-1], npoints);
  if(end_events[left_node_index] > start_events[right_node_index]) {
    swap(left_node_index, right_node_index);
  }
  findClusters(start_t, end_t, bin_size, left_node_index - npoints, results);
  findClusters(start_t, end_t, bin_size, right_node_index - npoints, results);
}

vector<double> EventAgglomerateClustering::binnedRangeQuery(int64_t time_begin, int64_t time_end, uint64_t bins, int hrd) {
  vector<double> results(bins);
  uint64_t bin_size(getBinSize(time_begin, time_end, bins));
  PRINTLOG("Got AGC binned range query");

  vector<int64_t> data_short_list;
  findClusters(time_begin, time_end, (int64_t)bin_size*hrd, npoints-2, data_short_list);

  for(long unsigned int i = 0; i < data_short_list.size(); i+=2) {
    int64_t start_time = data_short_list[i];
    int64_t end_time = data_short_list[i+1]; 
    
    if(end_time < time_begin || start_time > time_end) continue;


    if(start_time < time_begin) start_time = time_begin;
    if(start_time > time_end) continue;
    if(end_time < time_begin) continue;
    if(end_time > time_end) end_time = time_end;
    int64_t startingBin = getBinNumber(time_begin, time_end, bins, start_time);
    int64_t endingBin = getBinNumber(time_begin, time_end, bins, end_time);
    if(startingBin < 0 || endingBin < 0) continue;

    for(int64_t bin_it = startingBin+1;
    bin_it < endingBin && bin_it < (int64_t)bins && results[bin_it] < 0.5; 
    bin_it++)
    results[bin_it] = 1.0;
    
    if(startingBin < (int64_t)bins && results[startingBin] < 0.5) 
      results[startingBin] = (start_time % bin_size)?0.5:1.0;
    if(endingBin < (int64_t)bins && results[endingBin] < 0.5)
      results[endingBin] = (end_time % bin_size)?0.5:1.0;
  }
  
  data_short_list.clear();
  filters.clear();
  return results;
}


bool EventAgglomerateClustering::checkFilterSatisfied(const size_t node_id, const EventDict& filter) {
  try {
    for (const auto& [key, value] : filter) {
        if (!(hasAttributeAtIndex(node_id, key))) return false;
        if (!attribute_lists[node_id].at(key).count(get<size_t>(value))) return false;
    }
  } catch (...) {
    return true;
  }
  return true;
}
bool EventAgglomerateClustering::checkFiltersSatisfied(const size_t node_id) {
  for (const auto& filter : filters) {
    if (!checkFilterSatisfied(node_id, filter)) return false;
  }
  return true;
}

int64_t EventAgglomerateClustering::findNearestEvent(uint64_t cTime) {
  int64_t ret_result = -1;
  return_attribute_key = "ID";
  vector<int64_t> data_short_list;

  uint64_t bin_size(getBinSize(cTime, cTime+1, 1));
  findClusters(cTime, cTime+1, (int64_t)bin_size, npoints-2, data_short_list);
  if(data_short_list.size() > 0) ret_result = data_short_list[0];

  data_short_list.clear();
  return_attribute_key = "";
  return ret_result;
}

void AgglomerateClusters::insertDataIntoTree(double start_time,
                                             double end_time, 
                                             string track, 
                                             string primitive_name,
                                             string interval_id) {
  if(agglomerate_clusters.find(track) == agglomerate_clusters.end()) {
    agglomerate_clusters[track] = EventAgglomerateClustering();
    agglomerate_clusters[track].track = track;
  }
  agglomerate_clusters[track].insertDataIntoTree(start_time, end_time, primitive_name, interval_id);
  if(event_data_attributes.find("primitive") == event_data_attributes.end()) {
      event_data_attributes.insert(make_pair("primitive", StringIndexMapper()));
  }
  event_data_attributes["primitive"].insert(primitive_name);

  if(event_data_attributes.find("ID") == event_data_attributes.end()) {
      event_data_attributes.insert(make_pair("ID", StringIndexMapper()));
  }
  event_data_attributes["ID"].insert(interval_id);
}

void AgglomerateClusters::buildAllAggClusters() {
  for(auto it = agglomerate_clusters.begin(); it != agglomerate_clusters.end(); it++) {
    it->second.buildAggCluster(event_data_attributes);
    PRINTLOG("building agglomerate cluster for: " << it->first);
  }
}

LocDict AgglomerateClusters::binnedRangeQuery(int64_t time_begin, 
                                              int64_t time_end, 
                                              vector<string> &locations,
                                              uint64_t bins){
  LocDict locDict;
  PRINTLOG("Got AGC binned range query");

  try {
    for (size_t i = 0; i < filters.size(); i++) {
      for (const auto& [key, value] : filters[i]) {
        filters[i][key] = event_data_attributes[key].get_track_index(get<string>(value));
      }
    }
  } catch (...) {
    PRINTLOG("Error in converting filter attributes to indices");
  }

  
  chrono::steady_clock::time_point clock_begin = chrono::steady_clock::now();
  for (const string& c_loc_str : locations) {
    if(agglomerate_clusters.find(c_loc_str) == agglomerate_clusters.end()) {
      PRINTLOG("Track not found in agglomerate clusters " << c_loc_str);
      continue;
    }
    
    if(filters.size() > 0) agglomerate_clusters[c_loc_str].filters = filters;
    locDict[stol(c_loc_str)] = agglomerate_clusters[c_loc_str].binnedRangeQuery(time_begin, time_end, bins, horizontal_resolution_divisor);
  }
  chrono::steady_clock::time_point clock_end = chrono::steady_clock::now();
  // for (const auto& myPair : locDict) {
  //   cout << myPair.first << " = ";
  //   copy (myPair.second.begin(), myPair.second.end(), ostream_iterator<double>(cout,"\n") );
  //   cout << endl;
  // }
  filters.clear(); // automatically clear filters after query
  cout << "AGC," << "ds_window";
  if(filters.size() > 0) cout << "_cond";
  cout << "," << time_begin << "," << time_end << "," 
    << horizontal_resolution_divisor << ","
    << chrono::duration_cast<chrono::microseconds>(clock_end - clock_begin).count()
    << endl;
  return locDict;
}

string AgglomerateClusters::findNearestEvent(uint64_t cTime, uint64_t cLocation) {
  string ret_result("");
  string c_loc_str = to_string(cLocation);
  if(agglomerate_clusters.find(c_loc_str) == agglomerate_clusters.end()) {
    PRINTLOG("Track not found in agglomerate clusters " << c_loc_str);
    return ret_result;
  }
  int64_t result = agglomerate_clusters[c_loc_str].findNearestEvent(cTime);
  if(result >= 0) ret_result = event_data_attributes["ID"][result];
  return ret_result;
}

#ifdef TESTING
int main() {
    PRINTLOG("Agglomerate Clustering Starting!");
    // EventAgglomerateClustering agglomerate_clustering;
    AgglomerateClusters agglomerate_clustering;

    string input_file_path = "/mnt/c/Users/sayef/IdeaProjects/traveler-integrated/data_handler/cgal_libs/cgal_server/location_data/";
    fstream input_file(input_file_path + "9.location");
    if (!input_file.is_open()) {
        PRINTLOG("Failed to open input file");
        return -1;
    }
    uint64_t start_time, end_time;
    while(input_file >> start_time >> end_time) {
      string primitive_name = "first";
      string interval_id = "100000";
      if (start_time == 74755483) {
        primitive_name = "second";
        interval_id = "320000";
      }
      agglomerate_clustering.insertDataIntoTree(start_time, end_time, "9", primitive_name, interval_id);
    }
    input_file.close();
    // agglomerate_clustering.getDataSize();//6666739
    // PRINTLOG("Data inserted into the cluster");

    agglomerate_clustering.buildAllAggClusters();
    // vector<double> results = agglomerate_clustering.binnedRangeQuery(1074655386, 1246477086, 100, 1);

    // agglomerate_clustering.addPrimitiveFilter("second");
    // agglomerate_clustering.addIDFilter("320000");
    LocDict results = agglomerate_clustering.binnedRangeQuery(-1305029698, 2753780939, 9, 9);
    PRINTLOG("Results: ");
    for(const auto& [loc, values] : results) {
      cout << "Location: " << loc << " Values: ";
      for (const auto& value : values) {
          cout << setprecision(1) << value << " ";
      }
      cout << endl;
    }
    results = agglomerate_clustering.binnedRangeQuery(83188392, 83188393, 9, 9, 1);
    string i_id = agglomerate_clustering.findNearestEvent(83188387, 9);
    cout << "Interval ID: " << i_id << endl;
    // for (const auto& result : results) {
    //     cout << setprecision(1) << result << " ";
    // }
    // cout << endl;
    PRINTLOG("Agglomerate Clustering Finished!");
    return 0;
}
#endif