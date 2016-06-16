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
package backtype.storm.scheduler;

import backtype.storm.utils.Time;
import backtype.storm.Config;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import java.util.HashMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.config.RequestConfig; 
import org.apache.http.impl.client.HttpClientBuilder; 
import org.apache.http.client.HttpClient; 
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArtifactoryConfigLoader implements IConfigLoader {
    protected static final String ARTIFACTORY_URI="artifactory.config.loader.uri";
    protected static final String LOCAL_ARTIFACT_DIR="scheduler_artifacts";

    private static final Logger LOG = LoggerFactory.getLogger(ArtifactoryConfigLoader.class);

    @SuppressWarnings("rawtypes")
    private Map conf;
    private String host;
    private String filePath;
    private Integer port;
    private String uriString;
    private String location;
    private String localCacheDir;
    private int lastReturnedTime = 0;
    private Map lastReturnedValue;

    // Protected so we can override this in testing
    protected String doGet(String api, String artifact) {
        StringBuilder urlBuilder = new StringBuilder();
        URL localUrl;

        urlBuilder.append("http://").append(host).append(":").append(port).append("/artifactory/");
        if (api != null) {
            urlBuilder.append(api);
        }
        urlBuilder.append(artifact);
        String urlStr = urlBuilder.toString();
        try {
            localUrl = new URL(urlStr);
        } catch (MalformedURLException localMalformedURLException) {
            LOG.error("Misconfigured artifactory client:  URL={}", urlStr, localMalformedURLException);
            return null;
        }
        
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(10 * 1000).build();
        HttpClient httpclient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

        String returnValue;
        try {
            LOG.debug("About to issue a GET to {}", urlStr);
            HttpGet httpget = new HttpGet(urlStr);

            // Create a custom response handler
            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

                @Override
                public String handleResponse(
                        final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    } else {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }

            };
            String responseBody;
            try {
                responseBody = httpclient.execute(httpget, responseHandler);
            } catch (java.io.IOException e) {
                LOG.error("httpclient execute failed", e);
                responseBody=null;
            }
            returnValue = responseBody;
        } catch (Exception e) {
            LOG.error("Received exception while connecting to Artifactory", e);
            returnValue=null;
        }

        if (returnValue != null) {
            LOG.debug("Returning {}",returnValue);
        }
        return returnValue;
    }

    private JSONObject getArtifactMetadata() {
        String metadataStr = doGet("api/storage/", location);
        if (metadataStr == null) {
            return null;
        }
        JSONParser parser = new JSONParser();
        JSONObject returnValue;
        try {
            returnValue = (JSONObject) new JSONParser().parse(metadataStr);
        } catch (ParseException e) {
            LOG.error("Could not parse JSON string", e);
            return null;
        }

        return returnValue;
    }

    private String loadMostRecentArtifact() {
        // Is this a directory or is it a file?
        JSONObject json = getArtifactMetadata();
        if (json == null) {
            LOG.error("got null metadata");
            return null;
        }
        String downloadURI = (String) json.get("downloadUri");

        // This means we are pointing at a file.
        if (downloadURI != null) {
            // Then get it and return the file as string.
            String returnValue = doGet( null, location);
            saveInArtifactoryCache("latest.yaml", returnValue);
            return returnValue;
        }

        // This should mean that we were pointed at a directory.  
        // Find the most recent child and load that.
        JSONArray msg = (JSONArray) json.get("children");
        if (msg == null || msg.size() == 0) {
            LOG.error("Expected directory children not present");
            return null;
        }

        // Iterate over directory children and find most our artifact
        JSONObject newest = (JSONObject)msg.get(0);
        if (newest == null) {
            LOG.error("Failed to find most recent artifact uri in {}", location);
            return null;
        }

        for (int i = 1 ; i < msg.size() ; i++) {
            JSONObject child = (JSONObject) msg.get(i);
            if (child == null) {
                LOG.error("Failed to find most recent artifact uri in {}", location);
                return null;
            }
            if ( ((String)child.get("uri")).compareTo((String)newest.get("uri")) > 0) {  
                newest = child;
            }
        }
        String uri = (String) newest.get("uri");
        if (uri == null) {
            LOG.error("Expected directory uri not present");
            return null;
        }
        String returnValue =  doGet( null, location + uri);

        saveInArtifactoryCache(uri, returnValue);

        return returnValue;
    }

    private Map loadFromFile(File file) {
        Map ret;
        String pathString="Invalid";
        try {
            pathString = file.getCanonicalPath();
            Yaml yaml = new Yaml(new SafeConstructor());
            FileInputStream fis = new FileInputStream(file);
            ret = (Map) yaml.load(new InputStreamReader(fis));
        } catch (Exception e) {
            LOG.error("Failed to load from file {}", pathString, e);
            return null;
        }

        if (ret != null) {
            LOG.debug("returning a new map from file {}", pathString);
            lastReturnedTime = Time.currentTimeSecs();
            lastReturnedValue = new HashMap(ret);
            return lastReturnedValue;
        }

        return null;
    }

    private Map getLatestFromCache() {
        File dir = new File(localCacheDir);
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            LOG.error("No files in cache.");
            return null;
        }

        File mostRecentFile = files[0];
        for (int i = 1; i < files.length; i++) {
           if (mostRecentFile.lastModified() < files[i].lastModified()) {
               mostRecentFile = files[i];
           }
        }

        return loadFromFile(mostRecentFile);
    }

    private void saveInArtifactoryCache(String filename, String yamlData) {
        String localFileName = localCacheDir + File.separator + filename;

        try {
            FileOutputStream fos = new FileOutputStream(localFileName);
            fos.write(yamlData.getBytes());
            fos.flush();
            fos.close();
        } catch (IOException e) {
            LOG.error("Received exception when writing file {}", localFileName);
        }
    }

    private void makeArtifactoryCache() {
        // Just make sure approprite directories exist
        String localDirName = (String)conf.get(Config.STORM_LOCAL_DIR); 
        if (localDirName == null) {
            return;
        }

        // First make the cache dir
        localDirName = localDirName + File.separator + "nimbus" + File.separator + LOCAL_ARTIFACT_DIR;
        File dir = new File(String.valueOf(localDirName));
        if (! dir.exists()) {
            dir.mkdir();
        }

        // Now make the dir for our location
        localCacheDir = localDirName + File.separator + location.replaceAll("/", ".");
        dir = new File(String.valueOf(localCacheDir));
        if (! dir.exists()) {
            dir.mkdir();
        }
    }

    @Override
    public void prepare(Map conf) {
        this.conf = conf;
        uriString = (String)conf.get(ARTIFACTORY_URI);
        if (uriString != null) {
            URI uri = null;
            try {
                uri = new URI(uriString);
                if (uri.getScheme().equals("http")) {
                    host = uri.getHost();
                    port = uri.getPort();
                    location = uri.getPath();
                } else if (uri.getScheme().equals("file")) {
                    filePath = uri.getPath();
                }
            } catch (java.net.URISyntaxException e) {
                LOG.error("Failed to parse uri={}", uriString);
            }
        }
        makeArtifactoryCache();
    }

    @Override
    public Map load() {
        // Check for new file every 10 minutes
        if (lastReturnedValue != null && Time.currentTimeSecs() - lastReturnedTime < 600) {
            LOG.debug("returning our last map");
            return lastReturnedValue;
        }

        // If we are not configured properly, don't return a config map
        if (host != null && port != null && location != null) {

            // Get the most recent artifact as a String, and then parse the yaml
            String yamlConfig = loadMostRecentArtifact();

            // If we failed to get anything from Artifactory try to get it from our local cache
            if (yamlConfig == null) {
                return getLatestFromCache();
            }

            // Now parse it and return the map.
            Yaml yaml = new Yaml(new SafeConstructor());
            Map ret = (Map) yaml.load(yamlConfig);

            if (ret != null) {
                LOG.debug("returning a new map from Artifactory");
                lastReturnedTime = Time.currentTimeSecs();
                lastReturnedValue = new HashMap(ret);
                return lastReturnedValue;
            }
        }

        if (filePath != null) {
            File file = new File(filePath);
            Map ret = loadFromFile(file);

            if (ret != null) {
                LOG.debug("returning a new map from file");
                lastReturnedTime = Time.currentTimeSecs();
                lastReturnedValue = ret;
                return lastReturnedValue;
            }
        }

        return null;
    }
}
