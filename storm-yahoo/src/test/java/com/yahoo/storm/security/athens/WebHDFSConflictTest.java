package com.yahoo.storm.security.athens;

import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.junit.Test;
import org.junit.After;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebHDFSConflictTest {
    final static Logger LOG = LoggerFactory.getLogger(WebHDFSConflictTest.class);

    @Test
    public void testCooexist() throws Exception {
        AutoAthens aa = new AutoAthens(); //Make sure the classpath is setup
        Configuration hadoopConf = new Configuration();
        MiniDFSCluster dfscluster = new MiniDFSCluster.Builder(hadoopConf).build();
        try {
            dfscluster.waitActive();
            FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(hadoopConf, "webhdfs");
            Path dir = new Path("/test/");
            assertTrue(fs.mkdirs(dir));
        } finally {
            dfscluster.shutdown();
        }        
    }
}
