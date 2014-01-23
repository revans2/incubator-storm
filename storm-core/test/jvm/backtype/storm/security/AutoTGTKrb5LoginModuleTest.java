package backtype.storm.security;

import backtype.storm.security.auth.kerberos.AutoTGT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.topology.state.TransactionalState;

import javax.security.auth.Subject;
import java.util.Map;

/**
 * Created by kpatil on 1/23/14.
 */
public class AutoTGTKrb5LoginModuleTest {

    private static final Logger LOG = LoggerFactory.getLogger(AutoTGTKrb5LoginModuleTest.class);

    public static void main(String[] args) throws Exception {
        AutoTGT at = new AutoTGT();
        Map conf = new java.util.HashMap();
        conf.put("java.security.auth.login.config", args[0]);
        at.prepare(conf);
        Map<String, String> creds = new java.util.HashMap<String, String>();
        at.populateCredentials(creds);
        Subject s = new Subject();
        at.populateSubject(s, creds);
        LOG.info("Got a Subject " + s);
        TransactionalState.newUserState(conf, "TestAutoTGT");
    }
}
