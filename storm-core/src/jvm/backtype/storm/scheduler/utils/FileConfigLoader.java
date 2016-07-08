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
package backtype.storm.scheduler.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.HashMap;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileConfigLoader implements IConfigLoader {
    @SuppressWarnings("rawtypes")
    Map conf;
    protected static final String LOCAL_FILE_YAML="file.config.loader.local.file.yaml";
    private static final Logger LOG = LoggerFactory.getLogger(FileConfigLoader.class);

    @Override
    public void prepare(Map conf) {
        this.conf = conf;
    }

    private Map loadFromFile() {
        String localFileName = (String) conf.get(LOCAL_FILE_YAML);
        if (localFileName == null) {
            return null;
        }

        File theFile = new File(localFileName);
        if (theFile == null || !theFile.exists() || !theFile.canRead()) {
            return null;
        }

        Map ret;
        try {
            Yaml yaml = new Yaml(new SafeConstructor());
            FileInputStream fis = new FileInputStream(theFile);
            ret = (Map) yaml.load(new InputStreamReader(fis));
        } catch (Exception e) {
            LOG.error("Failed to load yaml from file" + localFileName, e);
            return null;
        }

        if (ret == null) {
            return null;
        }

        return new HashMap(ret);
    }

    @Override
    public Map load() {
        return loadFromFile();
    }
}
