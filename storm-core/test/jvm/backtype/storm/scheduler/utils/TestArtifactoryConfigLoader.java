/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backtype.storm.scheduler.utils;

import backtype.storm.Config;
import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;
import backtype.storm.scheduler.utils.ArtifactoryConfigLoader;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Files;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

public class TestArtifactoryConfigLoader {

    private static final Logger LOG = LoggerFactory.getLogger(TestArtifactoryConfigLoader.class);
    private static final String TMP_DIRNAME = "/tmp/artifactoryTest";

    private class TestClass extends ArtifactoryConfigLoader {
        String getData;
        HashMap<String, String> getDataMap = new HashMap<String, String>();

        public void setData(String api, String artifact, String data) {
            if (api == null) {
                getData = data;
            }  else {
                getDataMap.put(artifact, data);
            }
        }

        @Override
        protected String doGet(String api, String artifact) {
            if (api == null) {
                return getData;
            }
            return getDataMap.get(artifact);
        }
    };

    @Test
    public void testInvalid() {
        Config conf = new Config();
        TestClass testClass = new TestClass();
        testClass.prepare(conf);
        Map ret = testClass.load();
        Assert.assertNull("Unexpectedly returned not null", ret);
    }

    @Before
    public void createTempDir() throws Exception {
        File f = new File(TMP_DIRNAME);
        f.mkdir();
        f=new File(TMP_DIRNAME + "/nimbus");
        f.mkdir();
    }

    private void recursiveDeleteFile(File dir) {
        File[] listing = dir.listFiles();
        if (listing != null) {
            for (File f : listing) {
                recursiveDeleteFile(f);
            }
        }
        dir.delete();
    }

    @After
    public void removeTempDir() throws Exception {
        recursiveDeleteFile(new File(TMP_DIRNAME));
    }

    @Test
    public void testPointingAtDirectory() {
        // This is a test where we are configured to point right at an artifact dir
        Config conf = new Config();
        conf.put(ArtifactoryConfigLoader.ARTIFACTORY_URI, "http://bogushost.yahoo.com:9999/location/of/this/dir");
        conf.put(Config.STORM_LOCAL_DIR, TMP_DIRNAME);

        TestClass testClass = new TestClass();

        testClass.setData("Anything", "/location/of/this/dir",
            "{\"children\" : [ { \"uri\" : \"/20160621204337.yaml\", \"folder\" : false }]}" );
        testClass.setData(null, null, "{one: 1, two: 2, three: 3, four : 4}");
        testClass.prepare(conf);
        Map ret = testClass.load();

        Assert.assertNotNull("Unexpectedly returned null", ret);
        Assert.assertEquals(1, ret.get("one"));
        Assert.assertEquals(2, ret.get("two"));
        Assert.assertEquals(3, ret.get("three"));
        Assert.assertEquals(4, ret.get("four"));

        // Now let's load w/o setting up gets and we should still get valid map back
        TestClass tc2 = new TestClass();
        tc2.prepare(conf);
        Map ret2 = tc2.load();
        Assert.assertNotNull("Unexpectedly returned null", ret2);
        Assert.assertEquals(1, ret2.get("one"));
        Assert.assertEquals(2, ret2.get("two"));
        Assert.assertEquals(3, ret2.get("three"));
        Assert.assertEquals(4, ret2.get("four"));
    }

    @Test
    public void testArtifactUpdate() {
        // This is a test where we are configured to point right at an artifact dir
        Config conf = new Config();
        conf.put(ArtifactoryConfigLoader.ARTIFACTORY_URI, "http://bogushost.yahoo.com:9999/location/of/test/dir");
        conf.put(Config.STORM_LOCAL_DIR, TMP_DIRNAME);

        Time.startSimulating();

        try {
            TestClass testClass = new TestClass();

            testClass.setData("Anything", "/location/of/test/dir",
                "{\"children\" : [ { \"uri\" : \"/20160621204337.yaml\", \"folder\" : false }]}" );
            testClass.setData(null, null, "{one: 1, two: 2, three: 3}");
            testClass.prepare(conf);
            Map ret = testClass.load();

            Assert.assertNotNull("Unexpectedly returned null", ret);
            Assert.assertEquals(1, ret.get("one"));
            Assert.assertEquals(2, ret.get("two"));
            Assert.assertEquals(3, ret.get("three"));
            Assert.assertNull("Unexpectedly did not return null", ret.get("four"));

            // Now let's load w/o setting up gets and we should still get valid map back
            TestClass tc2 = new TestClass();
            tc2.prepare(conf);
            Map ret2 = tc2.load();
            Assert.assertNotNull("Unexpectedly returned null", ret2);
            Assert.assertEquals(1, ret2.get("one"));
            Assert.assertEquals(2, ret2.get("two"));
            Assert.assertEquals(3, ret2.get("three"));
            Assert.assertNull("Unexpectedly did not return null", ret2.get("four"));

            // Now let's update it, but not advance time.  Should get old map again.
            testClass.setData("Anything", "/location/of/test/dir",
                "{\"children\" : [ { \"uri\" : \"/20160621204999.yaml\", \"folder\" : false }]}");
            testClass.setData(null, null, "{one: 1, two: 2, three: 3, four: 4}");
            ret = testClass.load();
            Assert.assertNotNull("Unexpectedly returned null", ret);
            Assert.assertEquals(1, ret.get("one"));
            Assert.assertEquals(2, ret.get("two"));
            Assert.assertEquals(3, ret.get("three"));
            Assert.assertNull("Unexpectedly did not return null", ret.get("four"));

            // Re-load from cached' file.
            ret2 = tc2.load();
            Assert.assertNotNull("Unexpectedly returned null", ret2);
            Assert.assertEquals(1, ret2.get("one"));
            Assert.assertEquals(2, ret2.get("two"));
            Assert.assertEquals(3, ret2.get("three"));
            Assert.assertNull("Unexpectedly did not return null", ret2.get("four"));

            // Now, let's advance time.
            Time.advanceTime(11*60*1000);

            ret = testClass.load();
            Assert.assertNotNull("Unexpectedly returned null", ret);
            Assert.assertEquals(1, ret.get("one"));
            Assert.assertEquals(2, ret.get("two"));
            Assert.assertEquals(3, ret.get("three"));
            Assert.assertEquals(4, ret.get("four"));

            // Re-load from cached' file.
            ret2 = tc2.load();
            Assert.assertNotNull("Unexpectedly returned null", ret2);
            Assert.assertEquals(1, ret2.get("one"));
            Assert.assertEquals(2, ret2.get("two"));
            Assert.assertEquals(3, ret2.get("three"));
            Assert.assertEquals(4, ret2.get("four"));
        } finally {
            Time.stopSimulating();
        }
    }

    @Test
    public void testPointingAtSpecificArtifact() {
        // This is a test where we are configured to point right at a single artifact
        Config conf = new Config();
        conf.put(ArtifactoryConfigLoader.ARTIFACTORY_URI, "http://bogushost.yahoo.com:9999/location/of/this/artifact");
        conf.put(Config.STORM_LOCAL_DIR, TMP_DIRNAME);

        TestClass testClass = new TestClass();

        testClass.setData("Anything", "/location/of/this/artifact", "{ \"downloadUri\": \"anything\"}");
        testClass.setData(null, null, "{one: 1, two: 2, three: 3}");
        testClass.prepare(conf);
        Map ret = testClass.load();

        Assert.assertNotNull("Unexpectedly returned null", ret);
        Assert.assertEquals(1, ret.get("one"));
        Assert.assertEquals(2, ret.get("two"));
        Assert.assertEquals(3, ret.get("three"));

        // Now let's load w/o setting up gets and we should still get valid map back
        TestClass tc2 = new TestClass();
        tc2.prepare(conf);
        Map ret2 = tc2.load();
        Assert.assertNotNull("Unexpectedly returned null", ret2);
        Assert.assertEquals(1, ret2.get("one"));
        Assert.assertEquals(2, ret2.get("two"));
        Assert.assertEquals(3, ret2.get("three"));
    }

    @Test
    public void testValidFile() throws Exception {
        File temp = File.createTempFile("FileLoader", ".yaml");
        temp.deleteOnExit();
        Map<String, Integer> testMap = new HashMap<String, Integer>();

        testMap.put("a", 1);
        testMap.put("b", 2);
        testMap.put("c", 3);
        testMap.put("d", 4);

        Yaml yaml = new Yaml();
        FileWriter fw = new FileWriter(temp);
        yaml.dump(testMap, fw);
        fw.flush();
        fw.close();

        Config config = new Config();

        config.put(ArtifactoryConfigLoader.ARTIFACTORY_URI, "file://"+temp.getCanonicalPath());

        TestClass testClass = new TestClass();
        testClass.prepare(config);

        Map result = testClass.load();

        Assert.assertNotNull("Unexpectedly returned null", result);

        Assert.assertEquals("Maps are a different size", testMap.keySet().size(), result.keySet().size());

        for (String key : testMap.keySet() ) {
            Integer expectedValue = (Integer)testMap.get(key);
            Integer returnedValue = (Integer)result.get(key);
            Assert.assertEquals("Bad value for key=" + key, expectedValue, returnedValue);
        }

    }

    @Test
    public void testValidFileChange() throws Exception {
        Time.startSimulating();

        try {
            LOG.error("Doing testValidFileChange");
            File temp = File.createTempFile("FileLoader", ".yaml");
            temp.deleteOnExit();
            Map<String, Integer> testMap = new HashMap<String, Integer>();

            testMap.put("a", 1);
            testMap.put("b", 2);
            testMap.put("c", 3);
            testMap.put("d", 4);

            Yaml yaml = new Yaml();
            FileWriter fw = new FileWriter(temp);
            yaml.dump(testMap, fw);
            fw.flush();
            fw.close();

            Config config = new Config();
            config.put(ArtifactoryConfigLoader.ARTIFACTORY_URI, "file://"+temp.getCanonicalPath());

            TestClass testClass = new TestClass();
            testClass.prepare(config);

            Map result = testClass.load();

            Assert.assertNotNull("Unexpectedly returned null", result);

            Assert.assertEquals("Maps are a different size", testMap.keySet().size(), result.keySet().size());

            for (String key : testMap.keySet() ) {
                Integer expectedValue = (Integer)testMap.get(key);
                Integer returnedValue = (Integer)result.get(key);
                Assert.assertEquals("Bad value for key=" + key, expectedValue, returnedValue);
            }

            File temp2 = File.createTempFile("FileLoader", ".yaml");
            temp2.deleteOnExit();
            Map<String, Integer> testMap2 = new HashMap<String, Integer>();
            testMap2.put("a", 1);
            testMap2.put("b", 2);
            testMap2.put("c", 3);
            testMap2.put("d", 4);
            testMap2.put("e", 5);

            FileWriter fw2 = new FileWriter(temp2);
            yaml.dump(testMap2, fw2);
            fw2.flush();
            fw2.close();

            Config config2 = new Config();
            config2.put(ArtifactoryConfigLoader.ARTIFACTORY_URI, "file://"+temp2.getCanonicalPath());

            testClass.prepare(config2);

            Map result2 = testClass.load();

            Assert.assertNotNull("Unexpectedly returned null", result2);

            // Shouldn't change yet
            Assert.assertEquals("Maps are a different size", testMap.keySet().size(), result2.keySet().size());

            for (String key : testMap.keySet() ) {
                Integer expectedValue = (Integer)testMap.get(key);
                Integer returnedValue = (Integer)result2.get(key);
                Assert.assertEquals("Bad value for key=" + key, expectedValue, returnedValue);
            }

            Time.advanceTime(11*60*1000);

            // Now it should
            result2 = testClass.load();

            Assert.assertNotNull("Unexpectedly returned null", result2);

            // Shouldn't change yet
            Assert.assertEquals("Maps are a different size", testMap2.keySet().size(), result2.keySet().size());

            for (String key : testMap2.keySet() ) {
                Integer expectedValue = (Integer)testMap2.get(key);
                Integer returnedValue = (Integer)result2.get(key);
                Assert.assertEquals("Bad value for key=" + key, expectedValue, returnedValue);
            }

        } finally {
            Time.stopSimulating();
        }
    }
}
