package com.yahoo.storm.examples.hdfs;

import java.util.List;
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
import org.apache.storm.hdfs.bolt.sync.NullSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.bolt.HdfsBolt;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple topology that takes input from either DRPC or generated, and writes the data to HDFS.
 */
public class HDFSTopology {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSTopology.class);

    public static void main(String args[]) {
        Options opts = new Options();
        opts.addOption("drpc", true, "DRPC function to use, defaults to not use DRPC");
        opts.addOption("name", true, "topology name to use, defaults to \"default\"");
        opts.addOption(Option.builder("hdfsConf").argName("key=value")
                                .numberOfArgs(2)
                                .valueSeparator()
                                .desc( "use value for given HDFS config")
                                .build());
        opts.addOption("rotateSize", true, "rotate every so many MB, defaults to \"10\", set this lower for testing, the default spout is slow to fill this up");
        HelpFormatter formatter = new HelpFormatter();
        try {
            CommandLine line = new DefaultParser().parse(opts, args);

            String topologyName = line.getOptionValue("name","default");
            LOG.info("topology: {}", topologyName);

            List<String> more = line.getArgList();
            if (more.size() < 1) {
                throw new ParseException("An <output_path> must be supplied");
            }
            String outputPath = more.get(0);
            LOG.info("outputPath: {}", outputPath);
            Map<String, String> hadoopConf = new HashMap<String, String>((Map)line.getOptionProperties("hdfsConf"));
            String drpc = line.getOptionValue("drpc");
            float rotate = Float.valueOf(line.getOptionValue("rotateSize","10"));
            LOG.info("rotateSize: {}", rotate);
            setupStorm(outputPath, topologyName, hadoopConf, drpc, rotate);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            formatter.printHelp(HDFSTopology.class.getName()+" [options] <output_path>", opts);
            System.exit(1);
        } catch (Exception ex) {
            LOG.error("Error running topology", ex);
            System.exit(1);
        }
    }

    public static void setupStorm(String outputPath, String topology_name, Map<String, String> hadoopConf, String drpc, float rotate) throws Exception {
        //setup topology
        TopologyBuilder builder = new TopologyBuilder();
        if (drpc != null) {
          builder.setSpout("spout", new DRPCSpout(drpc), 1);
        } else {
          builder.setSpout("spout", new TestWordSpout(), 1);
        }

        SyncPolicy syncPolicy = new NullSyncPolicy();

        // rotate files every X MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(rotate, FileSizeRotationPolicy.Units.MB);

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
        LOG.info("Running with hadoop conf overrides of {}", hadoopConf);
        storm_conf.put("hdfs.config.override", hadoopConf);

        StormSubmitter.submitTopology(topology_name, storm_conf, builder.createTopology());
    }
}
