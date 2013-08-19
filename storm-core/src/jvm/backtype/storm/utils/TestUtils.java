package backtype.storm.utils;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import java.util.Map;

public class TestUtils extends Utils {

    public static void testSetupBuilder(CuratorFrameworkFactory.Builder
            builder, String zkStr, Map conf, ZookeeperAuthInfo auth)
    {
        setupBuilder(builder, zkStr, conf, auth);
    }

}
