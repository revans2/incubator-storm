package backtype.storm.utils;

import org.junit.Test;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.retry.ExponentialBackoffRetry;

import backtype.storm.Config;

public class UtilsTest {

  @Test
  public void testNewCuratorUsesExponentialBackoff() {
    @SuppressWarnings("unchecked")
    Map<String,Object> conf = (Map<String,Object>)Utils.readDefaultConfig();
    List<String> servers = new ArrayList<String>();
    servers.add("bogus_server");
    Object port = new Integer(42);
    CuratorFramework cf = Utils.newCurator(conf, servers, port);

    assertTrue(cf.getZookeeperClient().getRetryPolicy() instanceof ExponentialBackoffRetry);
  }
}
