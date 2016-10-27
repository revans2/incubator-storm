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

package backtype.storm.security.auth;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Automatically take a user's SSL files and serialize them into credentials to be sent to
 * the workers and then deserialized back into the same files.
 *
 * User is responsible for referencing them from the code as ./<filename>.
 */
public class AutoSSL implements IAutoCredentials {
    private static final Logger LOG = LoggerFactory.getLogger(AutoSSL.class);
    private Map conf;
    private String writeDir = "./";

    public static final String SSL_FILES_CONF = "ssl.credential.files";
    // used for testing only
    @VisibleForTesting
    public static final String SSL_WRITE_DIR_CONF = "ssl.credential.write.dir";

    public void prepare(Map conf) {
        this.conf = conf;
        writeDir = getSSLWriteDirFromConf(this.conf);
    }

    private String getSSLWriteDirFromConf(Map conf) {
        Object sslConf = conf.get(SSL_WRITE_DIR_CONF);
        if (sslConf == null) {
            return "./";
        }
        if (sslConf instanceof String) {
            return (String) sslConf;
        }
        throw new RuntimeException(
            SSL_WRITE_DIR_CONF + " is not set to something that I know how to use " + sslConf);
    }

    @VisibleForTesting
    Collection<String> getSSLFilesFromConf(Map conf) {
        Object sslConf = conf.get(SSL_FILES_CONF);
        if (sslConf == null) {
            LOG.info("No ssl files requested, if you want to use SSL please set {} to the " +
                    "list of files", SSL_FILES_CONF);
            return null;
        }
        Collection<String> sslFiles = null;
        if (sslConf instanceof Collection) {
            sslFiles = (Collection<String>) sslConf;
        } else if (sslConf instanceof String) {
            sslFiles = Arrays.asList(((String) sslConf).split(","));
        } else {
            throw new RuntimeException(
                SSL_FILES_CONF + " is not set to something that I know how to use " + sslConf);
        }
        return sslFiles;
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        try {
            Collection<String> sslFiles = getSSLFilesFromConf(conf);
            if (sslFiles == null) {
                return;
            }
            LOG.info("AutoSSL files: " + sslFiles);
            for (String inputFile : sslFiles) {
                serializeSSLFile(inputFile, credentials);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void serializeSSLFile(String readFile, Map<String, String> credentials) {
        try {
            FileInputStream in = new FileInputStream(readFile);
            LOG.debug("serializing ssl file: " + readFile);
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int length;
            while ((length = in.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            String resultStr = DatatypeConverter.printBase64Binary(result.toByteArray());

            File f = new File(readFile);
            LOG.debug("ssl read files is name " + f.getName());
            credentials.put(f.getName(), resultStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void deserializeSSLFile(String credsKey, String directory, 
        Map<String, String> credentials) {
        try {
            LOG.debug("deserializing ssl file with key: " + credsKey);
            String resultStr = null;

            if (credentials != null &&
                credentials.containsKey(credsKey) &&
                credentials.get(credsKey) != null)
            {
                resultStr = credentials.get(credsKey);
            }
            if (resultStr != null) {
                byte[] decodedData = DatatypeConverter.parseBase64Binary(resultStr);
                File f = new File(directory, credsKey);
                FileOutputStream fout = new FileOutputStream(f);
                fout.write(decodedData);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        populateSubject(subject, credentials);
    }

    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        LOG.info("AutoSSL populating credentials");
        Collection<String> sslFiles = getSSLFilesFromConf(conf);
        if (sslFiles == null) {
            LOG.debug("ssl files is null");
            return;
        }
        for (String outputFile : sslFiles) {
            deserializeSSLFile(new File(outputFile).getName(), writeDir,  credentials);
        }
    }
}
