package backtype.storm;

import java.util.Map;

import backtype.storm.StormSubmitter;

public class TestStormSubmitter extends StormSubmitter {

    /**
     * Default constructor to satisfy the compiler.
     */
    public TestStormSubmitter() {
        super();
    }

    public static Map prepareZookeeperAuthentication(Map conf) {
        return StormSubmitter.prepareZookeeperAuthentication(conf);
    }
}
