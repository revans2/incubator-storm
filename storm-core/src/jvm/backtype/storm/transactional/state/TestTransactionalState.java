package backtype.storm.transactional.state;

import java.util.List;
import java.util.Map;

import backtype.storm.utils.ZookeeperAuthInfo;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

/**
 * Facilitates testing of non-public methods in the parent class.
 */
public class TestTransactionalState extends TransactionalState {

    /**
     * Matching constructor in absence of a default constructor in the parent
     * class.
     */
    protected TestTransactionalState(Map conf, String id, Map componentConf, String subroot) {
        super(conf, id, componentConf, subroot);
    }

    public static void createNode(CuratorFramework curator, 
            String rootDir, byte[] data, List<ACL> acls, CreateMode mode)
            throws Exception {
       TransactionalState.createNode(curator, rootDir, data, acls, mode);
    }
}
