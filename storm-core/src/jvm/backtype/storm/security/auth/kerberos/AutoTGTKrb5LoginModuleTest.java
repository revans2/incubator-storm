package backtype.storm.security.auth.kerberos;

import backtype.storm.Config;
import backtype.storm.security.auth.kerberos.AutoTGT;
import backtype.storm.utils.Utils;
import backtype.storm.utils.ZookeeperAuthInfo;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.ProtectACLCreateModePathAndBytesable;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.topology.state.TransactionalState;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.util.*;

/**
 * Testing AutoTGTKrb5LoginModule to auto login into the ZK.
 */
public class AutoTGTKrb5LoginModuleTest {

    private static final Logger LOG = LoggerFactory.getLogger(AutoTGTKrb5LoginModuleTest.class);

    public static void main(String[] args) throws Exception {
        Map conf = new java.util.HashMap();
        conf.put("java.security.auth.login.config", args[0]);
        conf.put("transactional.zookeeper.root", "/transactions");
        conf.put("storm.zookeeper.connection.timeout", "15000");
        conf.put("storm.zookeeper.retry.interval", "1000");
        conf.put("storm.zookeeper.retry.intervalceiling.millis", "30000");
        conf.put("storm.zookeeper.retry.times", "5");
        conf.put("storm.zookeeper.root", "/storm");
        conf.put("storm.zookeeper.session.timeout", "20000");
        List<String> servers = new ArrayList<String>();
        servers.add(args[1]);

        AutoTGT at = new AutoTGT();
        at.prepare(conf);
        Map<String, String> creds = new java.util.HashMap<String, String>();
        at.populateCredentials(creds);
        Subject s = new Subject();
        at.populateSubject(s, creds);
        LOG.info("Got a Subject " + s);
        String id = "/transactions/TestAutoTGT-" + UUID.randomUUID();
        LOG.info("Adding id: " + id);

        CuratorFramework curator = Utils.newCuratorStarted(conf, servers, "2181", null);
        ProtectACLCreateModePathAndBytesable<String> builder = curator.create().creatingParentsIfNeeded();
        List<ACL> acls = new ArrayList<ACL>(ZooDefs.Ids.CREATOR_ALL_ACL);
        builder.withACL(acls).forPath(id);
        curator.close();
    }

}
