#include "eseman_kdt.h"

void test_event_tracks() {
    StringIndexMapper StringIndexMapper;
    StringIndexMapper.insert("12-14");
    StringIndexMapper.insert("11-35");
    StringIndexMapper.insert("12-51");
    StringIndexMapper.insert("10-20");
    StringIndexMapper.insert("25-15");

    PRINTLOG("StringIndexMapper size: " << StringIndexMapper.size());
    PRINTLOG("Track at index 4: " << StringIndexMapper[1]);
    string trackName = "12-352";
    PRINTLOG("Index of " << trackName << ": " << StringIndexMapper.get_track_index(trackName));

    // Custom sort: order by first number, then by the number after the dash
    StringIndexMapper.order_tracks([](const string& a, const string& b) {
        auto parse = [](const string& s) -> pair<int, int> {
            size_t dash = s.find('-');
            int first = stoi(s.substr(0, dash));
            int second = (dash != string::npos) ? stoi(s.substr(dash + 1)) : 0;
            return {first, second};
        };
        auto pa = parse(a);
        auto pb = parse(b);
        return pa < pb;
    });
    PRINTLOG("Tracks ordered.");
    StringIndexMapper.print_tracks();
    PRINTLOG("Index of " << trackName << ": " << StringIndexMapper.get_track_index(trackName));
}

// ESEmanNode functions
vector<string> EsemanNode::getAttributeKeys() {
    vector<string> keys;
    for (const auto& pair : attribute_lists) {
        keys.push_back(pair.first);
    }
    return keys;
}

void EsemanNode::addAttribute(const string& key, const int attr_index) {
    if(!hasAttribute(key)) {
      attribute_lists[key] = unordered_set<size_t>();
    }
    attribute_lists[key].insert(attr_index);
    // int v_index = attr_index / 64;
    // int v_bit = attr_index % 64;
    // uint64_t mask = 1ULL << v_bit;
    // while (attribute_lists[key].size() <= v_index) {
    //   attribute_lists[key].push_back(0);
    // }
    // uint64_t& value = attribute_lists[key][v_index];
    // value |= mask;
}

void EseManKDT::insertDataIntoTree(double start_time, double end_time, string track, string primitive_name, string interval_id) {
    size_t track_index = event_tracks.get_track_index(track);
    if(track_index > event_tracks.size()) {
        PRINTLOG("Track index out of range: " << track_index);
        return;
    } else if(track_index == event_tracks.size()) {
        event_tracks.insert(track);
        track_index = event_tracks.get_track_index(track);
        event_data_values.push_back(EventDictList());
    }
    event_data_values[track_index].push_back(EventDict{{"time",start_time},{"primitive", primitive_name},{"ID", interval_id}});
    event_data_values[track_index].push_back(EventDict{{"time",end_time},{"primitive", primitive_name},{"ID", interval_id}});
    
    if(event_data_attributes.find("primitive") == event_data_attributes.end()) {
        event_data_attributes.insert(make_pair("primitive", StringIndexMapper()));
    }
    event_data_attributes["primitive"].insert(primitive_name);

    if(event_data_attributes.find("ID") == event_data_attributes.end()) {
        event_data_attributes.insert(make_pair("ID", StringIndexMapper()));
    }
    event_data_attributes["ID"].insert(interval_id);
}

void EseManKDT::deleteTree(EsemanNode *node) {
    if (!node) return;
    
    if (node->hasLeftChild()) {
        deleteTree(node->left_node);
    }
    if (node->hasRightChild()) {
        deleteTree(node->right_node);
    }
    delete node;
}

inline bool EseManKDT::checkFilterSatisfied(const EsemanNode* node, const EventDict& filter) {
    try {
        for (const auto& [key, value] : filter) {
            if (!(node->hasAttribute(key))) return false;
            if (!node->attribute_lists.at(key).count(get<size_t>(value))) return false;
        }
    } catch (...) {
        return true;
    }
    return true;
}
inline bool EseManKDT::checkFiltersSatisfied(const EsemanNode* node) {
    for (const auto& filter : filters) {
        if (!checkFilterSatisfied(node, filter)) return false;
    }
    return true;
}

// This is following only the sliding midpoint rule.
string EseManKDT::constructKDTPerTrack(size_t start_index, size_t end_index, size_t track_index) {
    string result_uuid("");
    EventDictList& data_vector = event_data_values[track_index];
    if (start_index >= data_vector.size() || end_index >= data_vector.size() || start_index >= end_index) return result_uuid;
    
    EsemanNode* cur_node = new EsemanNode(getEventTime(data_vector[start_index]), getEventTime(data_vector[end_index]), track_index);
    if (start_index + 1 == end_index) {
        for (const auto& [key, indexes] : data_vector[start_index]) {
            if (key == "time") continue;
            size_t attr_index = event_data_attributes[key].get_track_index(get<string>(indexes));
            cur_node->addAttribute(key, attr_index);
        }
        saveNodeToLMDB(cur_node);
        result_uuid = cur_node->uuid;
        delete cur_node;
        return result_uuid;
    }

    string splitting_rule("FAIR");
    char* ntask_str = getenv("ESEMAN_SPLITTING_RULE");
    if(ntask_str != NULL) splitting_rule = string(ntask_str);

    size_t mid_index = start_index+2;
    if(splitting_rule == "MIDPOINT") {
        // sliding midpoint rule
        double mid_point = getEventTime(data_vector[start_index]) + (getEventTime(data_vector[end_index]) - getEventTime(data_vector[start_index])) / 2.0;
        mid_index = upper_bound(data_vector.begin() + start_index, data_vector.begin() + end_index + 1, 
            mid_point,
            [](double val, const EventDict& dict) {
            return val < getEventTime(dict);
            }) - data_vector.begin();
        if(mid_index%2 == 1)
            mid_index--;
        if(mid_index == start_index) mid_index = start_index + 2;
        if(mid_index >= end_index) {
            saveNodeToLMDB(cur_node);
            result_uuid = cur_node->uuid;
            delete cur_node;
            return result_uuid;
        }
        PRINTLOG("MIDPOINT Rule");
    } else if(splitting_rule == "MAX-DISTANCE") {
        // sliding midpoint of max distance rule
        double max_distance = 0;   
        for(size_t i = start_index+1; i < end_index; i+=2) {
            if(getEventTime(data_vector[i+1]) - getEventTime(data_vector[i]) > max_distance) {
                max_distance = getEventTime(data_vector[i+1]) - getEventTime(data_vector[i]);
                mid_index = i+1;
            }
        }
        if(mid_index >= end_index) {
            saveNodeToLMDB(cur_node);
            result_uuid = cur_node->uuid;
            delete cur_node;
            return result_uuid;
        }
        PRINTLOG("MAX-DISTANCE Rule");
    } else if(splitting_rule == "FAIR") {
        mid_index = start_index + (end_index + 1 - start_index) / 2;
        if (mid_index % 2 == 1) mid_index--;
        if(mid_index == start_index) mid_index = start_index + 2;
        if(mid_index >= end_index) {
            saveNodeToLMDB(cur_node);
            result_uuid = cur_node->uuid;
            delete cur_node;
            return result_uuid;
        }
        PRINTLOG("Fair Rule");
    }

    cur_node->left_child = constructKDTPerTrack(start_index, mid_index-1, track_index);
    cur_node->right_child = constructKDTPerTrack(mid_index, end_index, track_index);

    if (cur_node->hasLeftChild()) {
        EsemanNode* left_node = loadNodeFromLMDB(cur_node->left_child);
        if(left_node) {
            for (const auto& [key, indexes] : left_node->attribute_lists) {
                for (size_t index : indexes) {
                    cur_node->addAttribute(key, index);
                }
            }
            delete left_node;
        }
    }
    if (cur_node->hasRightChild()) {
        EsemanNode* right_node = loadNodeFromLMDB(cur_node->right_child);
        if(right_node) {
            for (const auto& [key, indexes] : right_node->attribute_lists) {
                for (size_t index : indexes) {
                    cur_node->addAttribute(key, index);
                }
            }
            delete right_node;
        }
    }
    
    saveNodeToLMDB(cur_node);
    result_uuid = cur_node->uuid;
    delete cur_node;
    return result_uuid;
}

// This is following only the sliding midpoint rule.
string EseManKDT::constructTwoDKDT(double start_time, double end_time, size_t start_track, size_t end_track, int depth) {
    string result_uuid("");
    if (start_track < 0 || end_track < 0 
        || start_track >= event_tracks.size() || end_track >= event_tracks.size() 
        || start_track > end_track || start_time > end_time) return result_uuid;

    if (start_track == end_track) {
        // If only one track, construct KDT for that track
        auto& data_vector = event_data_values[start_track];
        if(data_vector.size() == 0) return result_uuid;
        auto cmp_start = [this](const EventDict& dict, double t) { return getEventTime(dict) < t; };

        auto start_it = std::lower_bound(data_vector.begin(), data_vector.end(), start_time, cmp_start);
        auto end_it = std::lower_bound(data_vector.begin(), data_vector.end(), end_time, cmp_start);

        size_t start_index = std::distance(data_vector.begin(), start_it);
        size_t end_index = std::distance(data_vector.begin(), end_it);
        if(end_index >= data_vector.size()) end_index = data_vector.size() - 1;

        if (start_index > end_index || start_index >= data_vector.size() || end_index > data_vector.size() || end_index == 0) {
            return result_uuid;
        }
        EsemanNode* cur_node = nullptr;
        if (start_index % 2 == 1) { // odd index
            if (getEventTime(data_vector[start_index]) < start_time) start_index++;
            else {
                // create here a split node from start_time to start_index.time as a left child and the rest as a right child
                cur_node = new EsemanNode(start_time, end_time, start_track);

                EsemanNode* l_node = new EsemanNode(start_time, getEventTime(data_vector[start_index]), start_track);
                for (const auto& [key, indexes] : data_vector[start_index]) {
                    if (key == "time") continue;
                    size_t attr_index = event_data_attributes[key].get_track_index(get<string>(indexes));
                    l_node->addAttribute(key, attr_index);
                }
                saveNodeToLMDB(l_node);
                cur_node->left_child = l_node->uuid;
                delete l_node;

                start_index++;
                cur_node->right_child = constructKDTPerTrack(start_index, end_index, start_track);
            }
        } else { // even index
            if (getEventTime(data_vector[start_index]) < start_time) {
                // create here a split node from start_time to start_index+1.time as a left child and the rest as a right child
                cur_node = new EsemanNode(start_time, end_time, start_track);

                EsemanNode* l_node = new EsemanNode(start_time, getEventTime(data_vector[start_index+1]), start_track);
                for (const auto& [key, indexes] : data_vector[start_index+1]) {
                    if (key == "time") continue;
                    size_t attr_index = event_data_attributes[key].get_track_index(get<string>(indexes));
                    l_node->addAttribute(key, attr_index);
                }
                saveNodeToLMDB(l_node);
                cur_node->left_child = l_node->uuid;
                delete l_node;

                start_index++;
                cur_node->right_child = constructKDTPerTrack(start_index+1, end_index, start_track);
            } else {
                // do nothing
            }
        }

        if (end_index % 2 == 1) { // odd index
            if (getEventTime(data_vector[end_index]) > end_time) {
                // create here a split node from end_index-1.time to end_time as a right child and the rest as a left child
                cur_node = new EsemanNode(start_time, end_time, start_track);

                EsemanNode* r_node = new EsemanNode(getEventTime(data_vector[end_index-1]), end_time, start_track);
                for (const auto& [key, indexes] : data_vector[end_index-1]) {
                    if (key == "time") continue;
                    size_t attr_index = event_data_attributes[key].get_track_index(get<string>(indexes));
                    r_node->addAttribute(key, attr_index);
                }
                saveNodeToLMDB(r_node);
                cur_node->right_child = r_node->uuid;
                delete r_node;

                end_index--;
                cur_node->left_child = constructKDTPerTrack(start_index, end_index-1, start_track);
            }
            else {
                // do nothing
            }
        } else { // even index
            if (getEventTime(data_vector[end_index]) > end_time) end_index--;
            else {
                // create here a split node from end_index.time to end_time as a right child and the rest as a left child
                cur_node = new EsemanNode(start_time, end_time, start_track);

                EsemanNode* r_node = new EsemanNode(getEventTime(data_vector[end_index]), end_time, start_track);
                for (const auto& [key, indexes] : data_vector[end_index]) {
                    if (key == "time") continue;
                    size_t attr_index = event_data_attributes[key].get_track_index(get<string>(indexes));
                    r_node->addAttribute(key, attr_index);
                }
                saveNodeToLMDB(r_node);
                cur_node->right_child = r_node->uuid;
                delete r_node;

                end_index--;
                cur_node->left_child = constructKDTPerTrack(start_index, end_index, start_track);
            }
        }

        if (cur_node) {
            if (cur_node->hasLeftChild()) {
                EsemanNode* left_node = loadNodeFromLMDB(cur_node->left_child);
                if(left_node) {
                    for (const auto& [key, indexes] : left_node->attribute_lists) {
                        for (size_t index : indexes) {
                            cur_node->addAttribute(key, index);
                        }
                    }
                    delete left_node;
                }
            }
            if (cur_node->hasRightChild()) {
                EsemanNode* right_node = loadNodeFromLMDB(cur_node->right_child);
                if(right_node) {
                    for (const auto& [key, indexes] : right_node->attribute_lists) {
                        for (size_t index : indexes) {
                            cur_node->addAttribute(key, index);
                        }
                    }
                    delete right_node;
                }
            }

            saveNodeToLMDB(cur_node);
            result_uuid = cur_node->uuid;
            delete cur_node;
            return result_uuid;
        }
        return constructKDTPerTrack(start_index, end_index, start_track);
    }

    EsemanNode* cur_node = new EsemanNode(start_time, end_time, start_track);
    cur_node->end_track = end_track;
    if(depth % 2 == 0) {
        double max_start = std::numeric_limits<double>::max();
        double max_end = 0;
        for (size_t t = start_track; t <= end_track; ++t) {
            auto& data_vector = event_data_values[t];
            auto cmp_start = [this](const EventDict& dict, double tt) { return getEventTime(dict) < tt; };

            auto start_it = std::lower_bound(data_vector.begin(), data_vector.end(), start_time, cmp_start);
            auto end_it = std::lower_bound(data_vector.begin(), data_vector.end(), end_time, cmp_start);

            size_t start_index = std::distance(data_vector.begin(), start_it);
            size_t end_index = std::distance(data_vector.begin(), end_it);

            // if (start_index > end_index || start_index >= data_vector.size() || end_index >= data_vector.size() || end_index == 0) {
            // continue;
            // }
            if(start_index%2 == 1) max_start = start_time;
            else max_start = std::min(max_start, getEventTime(data_vector[start_index]));

            if(end_index%2 == 1) max_end = end_time;
            else if(end_index>0 && start_index+1<end_index) {
                end_index--;
                max_end = std::max(max_end, getEventTime(data_vector[end_index]));
            }
        }
        if(max_end > 0) {
            // Split by time
            cur_node->left_child = constructTwoDKDT(max_start, std::floor((max_start + max_end ) / 2), start_track, end_track, depth + 1);
            cur_node->right_child = constructTwoDKDT(std::floor((max_start + max_end ) / 2)+1, max_end, start_track, end_track, depth + 1);
        }
    } else {
        // Split by track
        cur_node->left_child = constructTwoDKDT(start_time, end_time, start_track, (start_track + end_track) >> 1, depth + 1);
        cur_node->right_child = constructTwoDKDT(start_time, end_time, ((start_track + end_track) >> 1) + 1, end_track, depth + 1);
    }

    if (cur_node->hasLeftChild()) {
        EsemanNode* left_node = loadNodeFromLMDB(cur_node->left_child);
        if(left_node) {
            for (const auto& [key, indexes] : left_node->attribute_lists) {
                for (size_t index : indexes) {
                    cur_node->addAttribute(key, index);
                }
            }
            delete left_node;
        }
    }
    if (cur_node->hasRightChild()) {
        EsemanNode* right_node = loadNodeFromLMDB(cur_node->right_child);
        if(right_node) {
            for (const auto& [key, indexes] : right_node->attribute_lists) {
                for (size_t index : indexes) {
                    cur_node->addAttribute(key, index);
                }
            }
            delete right_node;
        }
    }
    saveNodeToLMDB(cur_node);
    result_uuid = cur_node->uuid;
    delete cur_node;
    return result_uuid;
}

void EseManKDT::checkNodeAvailability(EsemanNode* c_node, EsemanNode* replace_node) {
    if(!replace_node || !c_node) return;
    if(c_node->hasLeftChild() && c_node->left_child == replace_node->uuid) {
        if(c_node->left_node) deleteTree(c_node->left_node);
        c_node->left_node = replace_node;
        replace_node = nullptr;
        PRINTLOG("reusing nodes");
    } else if(c_node->hasRightChild() && c_node->right_child == replace_node->uuid) {
        if(c_node->right_node) deleteTree(c_node->right_node);
        c_node->right_node = replace_node;
        replace_node = nullptr;
        PRINTLOG("reusing nodes");
    }
}

void EseManKDT::clearDeepNodesFromCache(EsemanNode* c_node) {
    if(c_node->hasLeftChild() && c_node->left_node != nullptr){
        deleteTree(c_node->left_node->left_node);
        c_node->left_node->left_node = nullptr;
        deleteTree(c_node->left_node->right_node);
        c_node->left_node->right_node = nullptr;
        PRINTLOG("clearing left node cache");
    }
    if(c_node->hasRightChild() && c_node->right_node != nullptr){
        deleteTree(c_node->right_node->left_node);
        c_node->right_node->left_node = nullptr;
        deleteTree(c_node->right_node->right_node);
        c_node->right_node->right_node = nullptr;
        PRINTLOG("clearing right node cache");
    }
}

// search logic
// if bin size is less than cluster length, then go down
// else return the start end point of the current cluster
// dfs on the start and end time query
// Stack-based iterative version of findClusters
void EseManKDT::findClusters(int64_t start_t, int64_t end_t, int64_t bin_size, 
                            EsemanNode* root, EsemanNode* replace_node,
                            vector<int64_t> &results, int depth) {

    if (!root) return;

    // Create a stack to store nodes with their depth
    struct StackItem {
        EsemanNode* node;
        int depth;
    };
    stack<StackItem> nodeStack;
    nodeStack.push({root, depth});

    while (!nodeStack.empty()) {
        nodes_visited++;
        auto current = nodeStack.top();
        nodeStack.pop();
        EsemanNode* c_node = current.node;
        int current_depth = current.depth;

        if (has_filter_query && !checkFiltersSatisfied(c_node)) continue;

        int64_t start_time = (int64_t)c_node->start_time;
        int64_t end_time = (int64_t)c_node->end_time;
        if (start_time >= end_t || end_time <= start_t) continue;

        checkNodeAvailability(c_node, replace_node);

        if (bin_size >= (end_time - start_time + 1)) {
            if (has_return_attribute_key) {
                if (!c_node->hasAttribute(return_attribute_key)) {
                    PRINTLOG("Attribute not found for key: " << return_attribute_key);
                    continue;
                }
                results.push_back((int64_t)(*c_node->attribute_lists.at(return_attribute_key).begin()));
            } else {
                results.push_back(start_time);
                results.push_back(end_time);
            }
            max_depth_reached = std::max(max_depth_reached, current_depth);
            // PRINTLOG("Cluster: " << " Start: " << start_time << ", End: " << end_time << ", Depth: " << current_depth);
            PRINTLOG("Cluster: " << " Start: " << start_time << ", End: " << end_time);
            continue;
        }

        if (!c_node->hasLeftChild() && !c_node->hasRightChild()) {
            if (start_time < start_t) {
                start_time = start_t;
            }
            if (end_time > end_t) {
                end_time = end_t;
            }
            if (has_return_attribute_key) {
                if (!c_node->hasAttribute(return_attribute_key)) {
                    PRINTLOG("Attribute not found for key: " << return_attribute_key);
                    continue;
                }
                results.push_back((int64_t)(*c_node->attribute_lists.at(return_attribute_key).begin()));
            } else {
                results.push_back(start_time);
                results.push_back(end_time);
            }
            max_depth_reached = std::max(max_depth_reached, current_depth);
            PRINTLOG("Cluster-Leaf: " << " Start: " << start_time << ", End: " << end_time);
            continue;
        }

        // Load children if not already loaded
        if (!c_node->right_node) c_node->right_node = loadNodeFromLMDB(c_node->right_child);
        if (!c_node->left_node) c_node->left_node = loadNodeFromLMDB(c_node->left_child);

        // Push right child first (so left child gets processed first when popped)
        if (c_node->right_node) {
            nodeStack.push({c_node->right_node, current_depth + 1});
        }
        if (c_node->left_node) {
            nodeStack.push({c_node->left_node, current_depth + 1});
        }
    }
}

vector<double> EseManKDT::binnedRangeQueryPerTrack(int64_t time_begin, 
                                        int64_t time_end,
                                        size_t track_index,
                                        uint64_t bins,
                                        EsemanNode* replace_node){
    vector<double> results(bins);
    uint64_t bin_size(getBinSize(time_begin, time_end, bins));

    vector<int64_t> data_short_list;
    findClusters(time_begin, time_end, (int64_t)bin_size*horizontal_resolution_divisor, 
                event_data_nodes[track_index], replace_node,
                data_short_list, 0);

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
    return results;
}

LocDict EseManKDT::binnedRangeQueryAllTracks(int64_t time_begin, 
                                            int64_t time_end, 
                                            size_t track_begin, 
                                            size_t track_end, uint64_t bins) {

    LocDict locDict;
    uint64_t bin_size(getBinSize(time_begin, time_end, bins));

    EsemanNode* root = event_data_nodes[0];
    if (!root) return locDict;

    // Create a stack to store nodes with their depth
    struct StackItem {
        EsemanNode* node;
        int depth;
    };
    stack<StackItem> nodeStack;
    nodeStack.push({root, 0});
    map<size_t, vector<pair<int64_t, int64_t>>> results;

    while (!nodeStack.empty()) {
        nodes_visited++;
        auto current = nodeStack.top();
        nodeStack.pop();
        EsemanNode* c_node = current.node;
        int current_depth = current.depth;

        if (has_filter_query && !checkFiltersSatisfied(c_node)) continue;

        int64_t start_time = (int64_t)c_node->start_time;
        int64_t end_time = (int64_t)c_node->end_time;
        if (start_time >= time_end || end_time <= time_begin) continue;
        if (c_node->start_track > track_end || c_node->end_track < track_begin) continue;

        // checkNodeAvailability(c_node, replace_node);

        if ((int64_t)bin_size >= (end_time - start_time + 1) 
            && c_node->start_track == c_node->end_track 
            && c_node->start_track >= track_begin 
            && c_node->end_track <= track_end) {
            // if (has_return_attribute_key) {
            //     if (!c_node->hasAttribute(return_attribute_key)) {
            //         PRINTLOG("Attribute not found for key: " << return_attribute_key);
            //         continue;
            //     }
            //     results.push_back((int64_t)(*c_node->attribute_lists.at(return_attribute_key).begin()));
            // } else {
            if (results.find(c_node->start_track) == results.end()) {
                results[c_node->start_track] = vector<pair<int64_t, int64_t>>();
            }
            int64_t id = -1;
            if (c_node->hasAttribute("ID")) {
                id = (int64_t)(*c_node->attribute_lists.at("ID").begin());
            }
            results[c_node->start_track].push_back(pair<int64_t, int64_t>(start_time,id));
            results[c_node->start_track].push_back(pair<int64_t, int64_t>(end_time,id));
            
            max_depth_reached = std::max(max_depth_reached, current_depth);
            PRINTLOG("Cluster: " << " Start: " << start_time << ", End: " << end_time << ", s_track: " << c_node->start_track << ", e_track: " << c_node->end_track);
            continue;
        }

        if (!c_node->hasLeftChild() && !c_node->hasRightChild()
            && c_node->start_track == c_node->end_track
            && c_node->start_track >= track_begin 
            && c_node->end_track <= track_end) {
            if (start_time < time_begin) {
                start_time = time_begin;
            }
            if (end_time > time_end) {
                end_time = time_end;
            }
            // if (has_return_attribute_key) {
            //     if (!c_node->hasAttribute(return_attribute_key)) {
            //         PRINTLOG("Attribute not found for key: " << return_attribute_key);
            //         continue;
            //     }
            //     results.push_back((int64_t)(*c_node->attribute_lists.at(return_attribute_key).begin()));
            // } else {
            if (results.find(c_node->start_track) == results.end()) {
                results[c_node->start_track] = vector<pair<int64_t, int64_t>>();
            }
            int64_t id = -1;
            if (c_node->hasAttribute("ID")) {
                id = (int64_t)(*c_node->attribute_lists.at("ID").begin());
            }
            results[c_node->start_track].push_back(pair<int64_t, int64_t>(start_time,id));
            results[c_node->start_track].push_back(pair<int64_t, int64_t>(end_time,id));

            max_depth_reached = std::max(max_depth_reached, current_depth);
            PRINTLOG("Cluster-Leaf: " << " Start: " << start_time << ", End: " << end_time << ", s_track: " << c_node->start_track << ", e_track: " << c_node->end_track);
            continue;
        }

        // Load children if not already loaded
        if (!c_node->right_node) c_node->right_node = loadNodeFromLMDB(c_node->right_child);
        if (!c_node->left_node) c_node->left_node = loadNodeFromLMDB(c_node->left_child);

        // Push right child first (so left child gets processed first when popped)
        if (c_node->right_node) {
            nodeStack.push({c_node->right_node, current_depth + 1});
        }
        if (c_node->left_node) {
            nodeStack.push({c_node->left_node, current_depth + 1});
        }
    }

    for (const auto& [track_index, intervals] : results) {
        vector<double> bins_vec(bins, 0.0);
        size_t i = 0;
        for (; i + 1 < intervals.size(); i += 2) {
            int64_t start_time = intervals[i].first;
            size_t j = i + 1;
            for (; j < intervals.size(); j += 2) {
                if(intervals[i].second != intervals[j].second)break;
            }
            int64_t end_time = intervals[j - 2].first;
            i = j - 3;

            if (end_time < time_begin || start_time > time_end) continue;
            if (start_time < time_begin) start_time = time_begin;
            if (end_time > time_end) end_time = time_end;

            int64_t startingBin = getBinNumber(time_begin, time_end, bins, start_time);
            int64_t endingBin = getBinNumber(time_begin, time_end, bins, end_time);
            if (startingBin < 0 || endingBin < 0) continue;

            for (int64_t bin_it = startingBin + 1; bin_it < endingBin && bin_it < (int64_t)bins && bins_vec[bin_it] < 0.5; bin_it++)
                bins_vec[bin_it] = 1.0;

            if (startingBin < (int64_t)bins && bins_vec[startingBin] < 0.5)
                bins_vec[startingBin] = (start_time % bin_size) ? 0.5 : 1.0;
            if (endingBin < (int64_t)bins && bins_vec[endingBin] < 0.5)
                bins_vec[endingBin] = (end_time % bin_size) ? 0.5 : 1.0;
        }
        locDict[stol(event_tracks[track_index])] = vector<double>(bins_vec.begin(), bins_vec.end());
        bins_vec.clear();
    }
    return locDict;
}

LocDict EseManKDT::binnedRangeQuery(int64_t time_begin, 
                                    int64_t time_end,
                                    vector<string> &locations,
                                    uint64_t bins){
    LocDict locDict;
    PRINTLOG("Got EseMan KDT binned range query");

    has_filter_query = false;
    try {
        for (size_t i = 0; i < filters.size(); i++) {
            for (const auto& [key, value] : filters[i]) {
                has_filter_query = true;
                filters[i][key] = event_data_attributes[key].get_track_index(get<string>(value));
            }
        }
    } catch (...) {
        PRINTLOG("Error in converting filter attributes to indices");
    }

    int total_nodes_visited = 0;
    max_depth_reached = 0;
    leafs_read = 0;
    has_return_attribute_key = false;
    chrono::steady_clock::time_point clock_begin = chrono::steady_clock::now();
    if(is_vertical_split) {
        sort(locations.begin(), locations.end(), [](const string& a, const string& b) {
            return stoi(a) < stoi(b);
        });
        size_t st_track = event_tracks.get_track_index(locations[0]);
        size_t en_track = event_tracks.get_track_index(locations[locations.size()-1]);
        locDict = binnedRangeQueryAllTracks(time_begin, time_end, st_track, en_track, bins);
        PRINTLOG("From vertical split");
    } else {
        for (const string& loc : locations) {
            size_t track_index = event_tracks.get_track_index(loc);
            if(track_index == event_tracks.size()) {
                PRINTLOG("Track not found in event tracks " << loc);
                continue;
            }
            nodes_visited = 0;
            EsemanNode* t_node = checkHotNodes(time_begin, time_end, track_index);
#ifdef _DEBUG
            chrono::steady_clock::time_point track_clock_begin = chrono::steady_clock::now();
#endif
            locDict[stol(loc)] = binnedRangeQueryPerTrack(time_begin, time_end, track_index, bins, t_node);
#ifdef _DEBUG
            chrono::steady_clock::time_point track_clock_end = chrono::steady_clock::now();
#endif
            total_nodes_visited = std::max(total_nodes_visited, nodes_visited);
            PRINTLOG("Track index: " << track_index << " " << event_tracks[track_index] << " " << leafs_read << " " << chrono::duration_cast<chrono::microseconds>(track_clock_end - track_clock_begin).count());
        }
    }
    chrono::steady_clock::time_point clock_end = chrono::steady_clock::now();

    filters.clear(); // automatically clear filters after query
    has_filter_query = false;

    string profiled_ds("ESEMAN");
    if(is_vertical_split) {
        profiled_ds = "ESEMAN_TWOD";
    }
    cout << profiled_ds << ",ds_window";
    if(filters.size() > 0) cout << "_cond";
    cout << "," << time_begin << "," << time_end << "," 
        << horizontal_resolution_divisor << ","
        << chrono::duration_cast<chrono::microseconds>(clock_end - clock_begin).count()
        << endl;
    return locDict;
}

string EseManKDT::findNearestEvent(uint64_t cTime, uint64_t cLocation) {
  string ret_result("");
  string c_loc_str = to_string(cLocation);
  size_t track_index = event_tracks.get_track_index(c_loc_str);
  

  int64_t result = -1;
  return_attribute_key = "ID";
  has_return_attribute_key = true;
  vector<int64_t> data_short_list;

  uint64_t bin_size(getBinSize(cTime, cTime+1, 1));
  findClusters(cTime, cTime+1, (int64_t)bin_size, event_data_nodes[track_index], nullptr, data_short_list, 0);
  if(data_short_list.size() > 0) result = data_short_list[0];
  if(result >= 0) ret_result = event_data_attributes["ID"][result];
  data_short_list.clear();
  return_attribute_key = "";
  has_return_attribute_key = false;
  return ret_result;
}

// this function checks if the already loaded nodes in the event_data_nodes are enough to satisfy the query range.
// this will return true if there is no need to fetch nodes from LMDB
// this will return false if we need to fetch more nodes from LMDB
// do this for each track seperately

// this will return nullptr if no loading require. 
// otherwise this will update event_data_nodes with the lowest common ancester and return the pointer to the previous value of event_data_nodes
EsemanNode* EseManKDT::checkHotNodes(double start_time, double end_time, size_t track_index) {
    // four scenarios
    // first, zoom in overlapping range
    // second, zoom out overlapping range
    // third, pan within short overlapping range
    // fourth, out of range

    // Take maximum of 0 or start_time to avoid negative times
    start_time = std::max(0.0, start_time);
    end_time = std::max(0.0, end_time);



    // first check completely out of range check.
    EsemanNode *root = event_data_nodes[track_index];
    double first_index_left = std::max(0.0, start_time - 2*(end_time - start_time));
    double first_index = start_time;
    // double second_index = ((end_time - start_time) / 3) + start_time;
    // double third_index = (2 * (end_time - start_time) / 3) + start_time;
    double fourth_index = end_time;
    double foruth_index_right = end_time + 2*(end_time - start_time);

    // fourth case, jump to different range (from the utilization view), complete out of range
    if(fourth_index < root->start_time || root->end_time < first_index) {
        // deleteTree(root);
        root = nullptr;
        event_data_nodes[track_index] = findNodeInTimeRange(eseman_node_uuids[track_index], first_index_left, foruth_index_right, nullptr);
        // cout << "fourth case" << endl;
    // } // second case, zoom out overlapping range
    // else if(first_index < root->start_time && root->end_time < fourth_index) {
    }// third case, partially overlapping range, either start or end overlaps
    else if(first_index < root->start_time || root->end_time < fourth_index) {
        if(root->uuid == eseman_node_uuids[track_index]) // already in the root, nothign to do
            return nullptr;
        EsemanNode *t_node = findNodeInTimeRange(eseman_node_uuids[track_index], first_index_left, foruth_index_right, root);
        if(root->uuid == t_node->uuid) // already in the cache, nothing to do
            return nullptr;
        event_data_nodes[track_index] = t_node;
        // cout << "third case" << endl;
    } // first case, zoom in overlapping range
    else {
        // EsemanNode *t_node = findNodeInTimeRange(eseman_node_uuids[track_index], first_index_left, foruth_index_right, root);
        // if(root->uuid == t_node->uuid) // already in the cache, nothing to do
        //     return nullptr;
        // event_data_nodes[track_index] = t_node;
        return nullptr;
    }
    return root;
}

void EseManKDT::printKDTDotPerTrack(size_t track_index) {
    ofstream dotFile("track_" + to_string(track_index) + ".dot");
    dotFile << "digraph G {" << endl;
    dotFile << "  label = \"Track " << event_tracks[track_index] << "\";" << endl;
    printKDTDotRecursive(eseman_node_uuids[track_index], dotFile);
    dotFile << "}" << endl;
    dotFile.close();
}

void EseManKDT::printKDTDotRecursive(string uuid, ofstream& dotFile) {
    if (uuid.empty()) return;
    EsemanNode *node = loadNodeFromLMDB(uuid);
    if(!node)return;
    dotFile << "  \"" << node->start_time << "," << node->end_time << "\" [label=\"[" << node->uuid << "]\"];" << endl;
    if (node->hasLeftChild()) {
        dotFile << "  \"" << node->uuid << "\" -> \"" << node->left_child << "\";" << endl;
        printKDTDotRecursive(node->left_child, dotFile);
    }
    if (node->hasRightChild()) {
        dotFile << "  \"" << node->uuid << "\" -> \"" << node->right_child << "\";" << endl;
        printKDTDotRecursive(node->right_child, dotFile);
    }
    delete node;
}

void EseManKDT::printKDTDot() {
    for(size_t i = 0; i < event_data_values.size(); ++i) {
        printKDTDotPerTrack(i);
    }
}

void EseManKDT::buildKDT() {
    string dataset_path = node_storage_base_path + "/" + dataset_id;
    string commands_remove_dir = "cd " + node_storage_base_path + " && mkdir -p " + dataset_id;
    int result = system(commands_remove_dir.c_str());
    if (result != 0) {
        PRINTLOG("Failed to create directory: " << dataset_path);
        return;
    }

    if(is_vertical_split) {
        vector<pair<string, EventDictList>> track_data_pairs;
        for (size_t i = 0; i < event_tracks.size(); ++i) {
            track_data_pairs.emplace_back(event_tracks[i], event_data_values[i]);
        }
        std::sort(track_data_pairs.begin(), track_data_pairs.end(),
            [](const pair<string, EventDictList>& a, const pair<string, EventDictList>& b) {
                return stoi(a.first) < stoi(b.first);
            });
        event_tracks.cleanMemory();
        event_data_values.clear();
        for (const auto& p : track_data_pairs) {
            event_tracks.insert(p.first);
            event_data_values.push_back(p.second);
        }
    }

    cleanNodesFromMemory(true);
    reloadNodesFromFile(false);

    if(is_vertical_split) {
        PRINTLOG("Building KDT with vertical split");
        if(eseman_node_uuids.empty()) eseman_node_uuids = vector<string>(1, "");

        double global_min = std::numeric_limits<double>::max();
        double global_max = std::numeric_limits<double>::lowest();
        for (const auto& data_vector : event_data_values) {
            if (!data_vector.empty()) {
                auto it_first = data_vector.front().find("time");
                auto it_last = data_vector.back().find("time");
                if (it_first != data_vector.front().end()) {
                    double t_min = std::get<double>(it_first->second);
                    if (t_min < global_min) global_min = t_min;
                }
                if (it_last != data_vector.back().end()) {
                    double t_max = std::get<double>(it_last->second);
                    if (t_max > global_max) global_max = t_max;
                }
            }
        }
        PRINTLOG("Global min time: " << global_min << ", max time: " << global_max);

        openWritePermLMDB();
        writeNodeUuidAtIndex(constructTwoDKDT(global_min, global_max, 0, event_tracks.size() - 1, 0), 0);
        closeWritePermLMDB();
        event_data_values.clear();
        PRINTLOG("Vertical split KDT build completed");
        return;
    }

    if(eseman_node_uuids.empty()) eseman_node_uuids = vector<string>(event_data_values.size(), "");
    char* ntask_str = getenv("ESEMAN_TASK_COUNT");
    int ntask = ntask_str ? atoi(ntask_str) : 0;

    char* procid_str = getenv("ESEMAN_TASK_ID");
    int procid = procid_str ? atoi(procid_str) : 0;

    size_t starting_chunk_index = 0;
    size_t ending_chunk_index = event_data_values.size() - 1;
    if(ntask) {
        size_t chunk_size = (event_data_values.size() + ntask - 1) / ntask;  // ceiling division
        starting_chunk_index = procid * chunk_size;
        ending_chunk_index = min((procid + 1) * chunk_size - 1, event_data_values.size() - 1);
    }
    PRINTLOG("Building KDT with (ntask, procid, start, end) : (" 
        << ntask << ","<< procid << ","
        << starting_chunk_index << "," << ending_chunk_index << ")");

    for (size_t i = starting_chunk_index; i <= ending_chunk_index; ++i) {
        if (event_data_values[i].empty()) continue;

        if(is_vertical_split == false) {
            openWritePermLMDB();
            writeNodeUuidAtIndex(constructKDTPerTrack(0, event_data_values[i].size() - 1, i), i);
            closeWritePermLMDB();
        }
        PRINTLOG("Constructing KDT for track index: " << event_tracks[i]);
        event_data_values[i].clear();
    }
    event_data_values.clear();
}

void EseManKDT::writeNodeUuidAtIndex(string new_uuid, size_t index) {
    string dataset_path = node_storage_base_path + "/" + dataset_id;
    eseman_node_uuids.clear();
    ifstream uuid_file(dataset_path + "/eseman_node_uuids.dat");
    if (uuid_file.is_open()) {
        int uuid_count;
        uuid_file >> uuid_count;
        for (int i = 0; i < uuid_count; i++) {
            string nuid;
            uuid_file >> nuid;
            eseman_node_uuids.push_back(nuid);
        }
        uuid_file.close();
    }
    if(!eseman_node_uuids.size()) eseman_node_uuids = vector<string>(is_vertical_split?1:event_data_values.size(), "");
    eseman_node_uuids[index] = new_uuid;

    ofstream w_uuid_file(dataset_path + "/eseman_node_uuids.dat");
    if (w_uuid_file.is_open()) {
        w_uuid_file << eseman_node_uuids.size() << "\n";
        for (const auto& cuid : eseman_node_uuids) {
            w_uuid_file << cuid << "\n";
        }
        w_uuid_file.close();
    }
}

void EseManKDT::deleteFromLMDB(const string& uuid) {
    if (uuid.empty()) return;

    MDB_val key;
    if(!openWritePermLMDB())return;

    // Set the key to delete
    key.mv_size = uuid.length();
    key.mv_data = (void*)uuid.c_str();

    // Attempt deletion
    int rc = mdb_del(txn, dbi, &key, nullptr);

    if (rc == MDB_NOTFOUND) {
        PRINTLOG("Key not found.");
    } else if (rc != 0) {
        PRINTLOG("Delete failed: " << mdb_strerror(rc));
    } else {
        PRINTLOG("Key deleted successfully.");
    }

    closeWritePermLMDB();
}
void EseManKDT::saveNodeToLMDB(const EsemanNode* node) {
    if (!node) return;

    MDB_val key, data;
    int rc;

    // Serialize node data
    ostringstream oss;
    oss << doubleToStringZeroPrecision(node->start_time) << " " 
        << doubleToStringZeroPrecision(node->end_time) << " "
        << node->start_track << " " << node->end_track << "\n";

    // Serialize attributes
    oss << node->attribute_lists.size() << "\n";
    for (const auto& attr : node->attribute_lists) {
        oss << attr.first << " " << attr.second.size() << "\n";
        for (int val : attr.second) {
            oss << val << " ";
        }
        oss << "\n";
    }

    // Save child UUIDs
    oss << node->left_child << "\n";
    oss << node->right_child << "\n";

    string serialized_data = oss.str();
    
    // Store in LMDB
    key.mv_data = (void*)node->uuid.c_str();
    key.mv_size = node->uuid.length();
    data.mv_data = (void*)serialized_data.c_str();
    data.mv_size = serialized_data.length();

    rc = mdb_put(txn, dbi, &key, &data, 0);
    if (rc) {
        PRINTLOG("mdb_put failed, error " << rc);
    }
}
EsemanNode* EseManKDT::loadNodeFromLMDB(const string& uuid) {
    if (uuid == "NULL" || uuid == "") return nullptr;

    MDB_val key, data;
    
    int rc;

    // Prepare key
    key.mv_data = (void*)uuid.c_str();
    key.mv_size = uuid.length();

    // Get data from LMDB
    rc = mdb_get(txn, dbi, &key, &data);
    if (rc) {
        PRINTLOG("mdb_get failed, error " << rc);
        return nullptr;
    }

    // Create new node and parse data
    EsemanNode* node = new EsemanNode();
    node->uuid = uuid;

    string serialized_data((char*)data.mv_data, data.mv_size);
    istringstream iss(serialized_data);

    // Parse node data
    iss >> node->start_time >> node->end_time 
        >> node->start_track >> node->end_track;

    // Parse attributes
    int attr_count;
    iss >> attr_count;
    
    for (int i = 0; i < attr_count; i++) {
        string key;
        int size;
        iss >> key >> size;
        
        for (int j = 0; j < size; j++) {
            int val;
            iss >> val;
            node->attribute_lists[key].insert(val);
        }
    }

    // Parse child UUIDs
    string left_uuid, right_uuid;
    iss >> left_uuid >> right_uuid;

    node->left_child = left_uuid;
    node->right_child = right_uuid;
    leafs_read++;
    // PRINTLOG("Loaded from LMDB with uuid: " << uuid);
    return node;
}

void EseManKDT::cleanNodesFromMemory(bool is_store_existing) {
    if (is_store_existing) {
        PRINTLOG("eseman uuid size " << eseman_node_uuids.size());
        // since removing the directory is expensive operation, dont do it here.
        // string commands_remove_dir = "cd " + node_storage_base_path + " && rm -rf " + dataset_id + " && mkdir -p " + dataset_id;
        string commands_remove_dir = "cd " + node_storage_base_path + " && mkdir -p " + dataset_id;
        string dataset_path = node_storage_base_path + "/" + dataset_id;
        int result = system(commands_remove_dir.c_str());
        if (result != 0) {
            PRINTLOG("Failed to create directory: " << dataset_path);
            return;
        }
        // Save event_data_attributes to file
        ofstream attr_file(dataset_path + "/event_data_attributes.dat");
        if (attr_file.is_open()) {
            // Write number of attributes
            attr_file << event_data_attributes.size() << "\n";
            for (const auto& [key, mapper] : event_data_attributes) {
                // Write key and number of tracks
                attr_file << key << " " << mapper.size() << "\n";
                // Write each track
                for (size_t i = 0; i < mapper.size(); i++) {
                    attr_file << mapper[i] << "\n";
                }
            }
            attr_file.close();
        }
        // Save event_tracks to file
        ofstream tracks_file(dataset_path + "/event_tracks.dat");
        if (tracks_file.is_open()) {
            tracks_file << event_tracks.size() << "\n";
            for (size_t i = 0; i < event_tracks.size(); i++) {
                tracks_file << event_tracks[i] << "\n";
            }
            tracks_file.close();
        }
        // // Save eseman_node_uuids to file
        // ofstream uuid_file(dataset_path + "/eseman_node_uuids.dat");
        // if (uuid_file.is_open()) {
        //     uuid_file << eseman_node_uuids.size() << "\n";
        //     for (const auto& uuid : eseman_node_uuids) {
        //         uuid_file << uuid << "\n";
        //     }
        //     uuid_file.close();
        // }
    }
    event_data_nodes.clear();
    event_data_attributes.clear();
    event_tracks.cleanMemory();
    // eseman_node_uuids.clear();
}

bool EseManKDT::reloadNodesFromFile(bool is_load_attributes) {
    try {
        
        PRINTLOG("clearing memoery and reloading");
        string dataset_path = node_storage_base_path + "/" + dataset_id;
        // Load event_data_attributes from file
        event_data_attributes.clear();
        ifstream attr_file(dataset_path + "/event_data_attributes.dat");
        if (attr_file.is_open()) {
            int attr_count;
            attr_file >> attr_count;
            for (int i = 0; i < attr_count; i++) {
                string key;
                int track_count;
                attr_file >> key >> track_count;
                event_data_attributes.insert(make_pair(key, StringIndexMapper()));
                for (int j = 0; j < track_count; j++) {
                    string track;
                    attr_file >> track;
                    event_data_attributes[key].insert(track);
                }
            }
            attr_file.close();
        }

        event_tracks.cleanMemory();
        // Load event_tracks from file
        ifstream tracks_file(dataset_path + "/event_tracks.dat");
        if (tracks_file.is_open()) {
            int track_count;
            tracks_file >> track_count;
            for (int i = 0; i < track_count; i++) {
                string track;
                tracks_file >> track;
                event_tracks.insert(track);
            }
            tracks_file.close();
        }

        // Load eseman_node_uuids from file
        eseman_node_uuids.clear();
        ifstream uuid_file(dataset_path + "/eseman_node_uuids.dat");
        if (uuid_file.is_open()) {
            int uuid_count;
            uuid_file >> uuid_count;
            for (int i = 0; i < uuid_count; i++) {
                string uuid;
                uuid_file >> uuid;
                eseman_node_uuids.push_back(uuid);
            }
            uuid_file.close();
        }

        if(is_load_attributes) {
            // Load each node from file
            event_data_nodes.clear();
            for (string node_uid : eseman_node_uuids) {
                EsemanNode* node = findNodeInTimeRange(node_uid, -1, numeric_limits<int64_t>::max(), nullptr);
                if (node) {
                    event_data_nodes.push_back(node);
                    // PRINTLOG("Loaded node with Grand root UUID: " << node_uid << " and current UUID: " << node->uuid);
                } else {
                    // PRINTLOG("Failed to load node with UUID: " << node_uid << " and current UUID: " << node->uuid);
                }
            }

            // // Iterate over event_tracks vector
            // for(size_t i = 0; i < (is_vertical_split?1:event_tracks.size()); i++) {
            //     binnedRangeQueryPerTrack(event_data_nodes[i]->start_time,
            //                             event_data_nodes[i]->end_time,
            //                             i,
            //                             4000,
            //                             nullptr);
            // }
        }
        return true;
    } catch (const exception& e) {
        PRINTLOG("Error while reloading nodes from file: " << e.what());
    }
    return false;
}

EsemanNode* EseManKDT::findNodeInTimeRange(string uuid, double s_time, double e_time, EsemanNode* c_root) {
    EsemanNode* root = c_root;
    if(!c_root || c_root->uuid != uuid) {
        root = loadNodeFromLMDB(uuid);
    }
    if (!root) return root;

    struct StackItem {
        EsemanNode* node;
        string next_uuid;
        bool is_root;
    };
    stack<StackItem> nodeStack;
    nodeStack.push({root, "", true});

    EsemanNode* result = nullptr;
    while (!nodeStack.empty()) {
        auto current = nodeStack.top();
        nodeStack.pop();
        EsemanNode* currentNode = current.node;

        if(currentNode->hasLeftChild() && !currentNode->isLeftChildCached()) 
            currentNode->left_node = loadNodeFromLMDB(currentNode->left_child);
        if(currentNode->hasRightChild() && !currentNode->isRightChildCached()) 
            currentNode->right_node = loadNodeFromLMDB(currentNode->right_child);

        if (currentNode->hasLeftChild() && currentNode->left_node->end_time > s_time && 
            currentNode->hasRightChild() && currentNode->right_node->start_time < e_time) {
            result = currentNode;
            break;
        } 
        else if (currentNode->hasRightChild() && currentNode->right_node->start_time > e_time) {
            string next_uuid = currentNode->left_child;
            EsemanNode* next_node = currentNode->left_node;
            
            if(currentNode != c_root && !current.is_root) {
                next_node = nullptr;
                delete currentNode;
            }
            
            if(next_node) {
                nodeStack.push({next_node, "", false});
            } else {
                nodeStack.push({loadNodeFromLMDB(next_uuid), "", false});
            }
        }
        else if (currentNode->hasLeftChild() && currentNode->left_node->end_time < s_time) {
            string next_uuid = currentNode->right_child;
            EsemanNode* next_node = currentNode->right_node;
            
            if(currentNode != c_root && !current.is_root) {
                next_node = nullptr;
                delete currentNode;
            }
            
            if(next_node) {
                nodeStack.push({next_node, "", false});
            } else {
                nodeStack.push({loadNodeFromLMDB(next_uuid), "", false});
            }
        }
        else if (s_time < currentNode->start_time && currentNode->end_time < e_time) {
            result = currentNode;
            break;
        }
        else {
            result = currentNode;
            break;
        }
    }

    return result;
}

 // four scenarios
// first, zoom in overlapping range
// second, zoom out overlapping range
// third, pan within short overlapping range
// fourth, out of range

void test_cases_for_memory_check(EseManKDT *kdt) {
    // exact same range
    cout << "=========== TESTING EXACT SAME RANGE =================" << endl;
    vector<string> locations = {"2"};
    // LocDict result = kdt->binnedRangeQuery(-1305029698, 2753780939, 
    //                         locations,
    //                         100);
    LocDict result = kdt->binnedRangeQuery(1307411956, 1445679055,
                            locations,
                            10);
    // Print LocDict result
    for (const auto& [track_index, bins_vec] : result) {
        cout << "Track: " << track_index << " -> ";
        for (double val : bins_vec) {
            cout << val << " ";
        }
        cout << endl;
    }
    // kdt->binnedRangeQuery(-1305029698, 2753780939, 
    //                         locations,
    //                         4000);
    cout << "======================================================" << endl;

    // first, zoom in overlapping range
    // cout << "=========== TESTING ZOOM IN AND PANNING =================" << endl;
    // kdt->binnedRangeQuery(1332325382,1333479725, 
    //                         locations,
    //                         4000);
    // kdt->binnedRangeQuery(1332308652,1333462998,
    //                         locations,
    //                         4000);
    // cout << "======================================================" << endl;

    // // second, zoom out overlapping range
    // cout << "=========== TESTING PAN and ZOOM OUT =================" << endl;
    // kdt->binnedRangeQuery(1214016207, 1297864259, 
    //                         9, 9, 
    //                         100);
    // kdt->binnedRangeQuery(1278029701, 1375992785, 
    //                         9, 9, 
    //                         100);
    // cout << "======================================================" << endl;

    // // third, zoom out overlapping range
    // cout << "=========== TESTING OUT OF RANGE =================" << endl;
    // kdt->binnedRangeQuery(1380056092, 1399938945, 
    //                         9, 9, 
    //                         100);
    // cout << "======================================================" << endl;
}

void test_KDT_build() {
    EseManKDT *kdt = new EseManKDT();
    kdt->node_storage_base_path = "/mnt/d/tmp";

    kdt->is_vertical_split = true;
    kdt->setDatasetID("test_dataset");

    


    // string input_file_path = "/mnt/c/Users/sayef/IdeaProjects/traveler-integrated/data_handler/cgal_libs/cgal_server/location_data/";
    // fstream input_file(input_file_path + "9.location");
    // if (!input_file.is_open()) {
    //     PRINTLOG("Failed to open input file");
    //     return;
    // }
    // uint64_t start_time, end_time;
    // while(input_file >> start_time >> end_time) {
    //     string primitive_name = "first";
    //     if (start_time == 74755483) {
    //         primitive_name = "second";
    //     }
    //     string interval_id = "100000";
    //     if (start_time == 74755483) {
    //         interval_id = "320000";
    //     }
    //     kdt->insertDataIntoTree(start_time, end_time, "9", primitive_name, interval_id);
    // }
    // input_file.close();

    // fstream input_file1(input_file_path + "1.location");
    // if (!input_file1.is_open()) {
    //     PRINTLOG("Failed to open input file");
    //     return;
    // }
    // while(input_file1 >> start_time >> end_time) {
    //     kdt->insertDataIntoTree(start_time, end_time, "1", "first", "iid_1");
    // }
    // input_file1.close();

    // fstream input_file2(input_file_path + "2.location");
    // if (!input_file2.is_open()) {
    //     PRINTLOG("Failed to open input file");
    //     return;
    // }
    // while(input_file2 >> start_time >> end_time) {
    //     string primitive_name = "first";
    //     if (start_time == 74755483) {
    //         primitive_name = "second";
    //     }
    //     kdt->insertDataIntoTree(start_time, end_time, "2", primitive_name, "iid_1");
    // }
    // input_file2.close();

    // kdt->buildKDT();



    kdt->openReadOnlyLMDB();
    kdt->reloadNodesFromFile(true);
    test_cases_for_memory_check(kdt);
    // string i_id = kdt->findNearestEvent(83188393, 9);
    kdt->closeReadOnlyLMDB();
    // cout << "Interval ID: " << i_id << endl;
    

    // kdt.printKDTDot();
    // kdt.printKDTDot();
    // kdt.binnedRangeQuery(1, 1200, 
    //                     12, 12, 
    //                     100);
    // kdt->addPrimitiveFilter("first");
    // kdt->addIDFilter("320000");
    // kdt->cleanNodesFromMemory(true);

    // int cm;
    // cin >> cm;
    
    // PRINTLOG("ESEman KDT cleaned from memory, now reloading from file");
    // cin >> cm;
    // PRINTLOG("Reloading finished");
    // cin >> cm;
    // kdt->binnedRangeQuery(1144746004, 1399938545, 
    //                         9, 9, 
    //                         10);
    // string cc_id = kdt->findNearestEvent(83188390, 9);
    // cout << "Interval ID: " << cc_id << endl;
}

// void find_bitwise_insertion_index(int num) {
//     int first = num / 64;
//     int second = num % 64;
//     PRINTLOG("Finding bitwise insertion index for number: " << first << " " << second);

// }

// void test_bitwise_insertion() {
//     srand(time(nullptr));  // Seed with current time
//     int max_limit = 1000;// INT_MAX;
//     vector<int> numbers = {0, 32, 63, 64, 65, 127, 128, 129, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025};
//     // for (int i = 0; i < 10; i++) {
//     //     numbers.push_back(rand() % max_limit);  // Generate numbers in range [0, INT_MAX)
//     // }
//     for (int num : numbers) {
//         PRINTLOG("Number: " << num);
//         find_bitwise_insertion_index(num);
//     }

// }

void test_lmdb() {

}

#ifdef TESTING
int main() {
    PRINTLOG("hello inside eseman kdt");
    // test_event_tracks();
    // PRINTLOG("Printing resutls from RAM before cleaning");
    test_KDT_build();
    // test_lmdb();
    // test_bitwise_insertion();
    PRINTLOG("Eseman KDT finished!");
    return 0;
}
#endif