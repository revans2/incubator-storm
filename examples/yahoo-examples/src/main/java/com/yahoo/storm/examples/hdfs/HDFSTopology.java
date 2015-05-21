package com.yahoo.storm.examples.hdfs;

import java.util.Map;
import java.util.HashMap;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.testing.TestWordSpout;

import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.bolt.HdfsBolt;

import org.apache.log4j.Logger;

public class HDFSTopology {
    private static Logger LOG = Logger.getLogger(HDFSTopology.class);

    public static void main(String args[]) {
        HDFSTopology app = new HDFSTopology();
        try {
            String topologyName = args[0];
            LOG.info("topology: " + topologyName);

            String outputPath = args[1];
            LOG.info("outputPath: " + outputPath);
            Map<String, String> hadoopConf = new HashMap<String, String>();
            for (int i = 2; i < args.length; i++) {
                String [] parts = args[i].split("=",2);
                if (parts.length != 2) {
                    throw new IllegalArgumentException("hadoop config "+args[i]+" does not have a '=' in it");
                }
                hadoopConf.put(parts[0], parts[1]);
            }

            app.setupStorm(outputPath, topologyName, hadoopConf);
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.error(ex.getMessage());
        }
    }

    public void setupStorm(String outputPath, String topology_name, Map<String, String> hadoopConf) throws Exception {
        //setup topology
        TopologyBuilder builder = new TopologyBuilder();
        //This is a bit of a hack, we probably should just use command line arguments instead
        String drpc = hadoopConf.get("drpc");
        if (drpc != null) {
          builder.setSpout("spout", new DRPCSpout(drpc), 1);
        } else {
          builder.setSpout("spout", new TestWordSpout(), 1);
        }

        // sync the filesystem after every 100 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(100);

        // rotate files every 1 KB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.KB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(outputPath)
                .withExtension(".txt");

        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("\t");

        HdfsBolt bolt = new HdfsBolt()
                .withConfigKey("hdfs.config.override")
                .withFsUrl(outputPath)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
        builder.setBolt("hdfs", bolt, 1).shuffleGrouping("spout");

        if (drpc != null) {
            builder.setBolt("rr", new ReturnResults()).globalGrouping("spout");
        }

        Config storm_conf = new Config();
        storm_conf.setDebug(true);
        storm_conf.setNumWorkers(1);
        storm_conf.put("hdfs.config.override", hadoopConf);

        StormSubmitter.submitTopology(topology_name, storm_conf, builder.createTopology());
    }
}
