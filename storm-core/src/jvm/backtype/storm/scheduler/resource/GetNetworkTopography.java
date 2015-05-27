/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backtype.storm.scheduler.resource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//TODO: Extract code from Hadoop to auto extract network information
public class GetNetworkTopography {
    private final static String NETWORK_DATA_FILE = "../data/NetworkData";
    private static final Logger LOG = LoggerFactory
            .getLogger(GetNetworkTopography.class);
    File networkDataFile;
    Map<String, List<String>> clusteringInfo;

    public GetNetworkTopography() {
        this.clusteringInfo = new HashMap<String, List<String>>();
    }

    public Map<String, List<String>> getClusterInfo() {
        try {
            // read the json file
            FileReader reader = new FileReader(NETWORK_DATA_FILE);

            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(reader);

            JSONObject networkData = (JSONObject) jsonObject.get("NetworkData");
            JSONArray clusterInfo = (JSONArray) networkData.get("ClusterInfo");

            for (int i = 0; i < clusterInfo.size(); i++) {
                JSONObject cluster = (JSONObject) clusterInfo.get(i);
                String cluster_num = (String) cluster.get("cluster");
                JSONArray nodes = (JSONArray) cluster.get("nodes");
                ArrayList<String> clus = new ArrayList<String>();
                for (int j = 0; j < nodes.size(); j++) {
                    clus.add((String) nodes.get(j));
                }
                this.clusteringInfo.put(cluster_num, clus);
            }
        } catch (ParseException e) {
            LOG.error(e.toString());
        } catch (FileNotFoundException e) {
            LOG.error(e.toString());
        } catch (IOException e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
        return this.clusteringInfo;
    }
}
