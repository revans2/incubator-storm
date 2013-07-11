package backtype.storm.security.auth.authorizer;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;

import backtype.storm.Config;
import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.security.auth.ReqContext;
import backtype.storm.security.auth.authorizer.SimpleWhitelistAuthorizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An authorization implementation that simply checks a whitelist of users that
 * are allowed to use the cluster.
 */
public class SimpleACLAuthorizer extends SimpleWhitelistAuthorizer implements IAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleACLAuthorizer.class);
    public static String ACL_USERS_CONF = "storm.auth.simple-acl.users";
    public static String ACL_USERS_COMMANDS_CONF = "storm.auth.simple-acl.users.commands";
    public static String ACL_SUPERVISORS_CONF = "supervisor.supervisors";
    public static String ACL_SUPERVISORS_COMMANDS_CONF = "supervisor.supervisors.commands";
    public static String ACL_ADMINS_CONF = "storm.auth.simple-acl.admins";
    public static String ACL_TOPOUSERS_WHITELIST_CONF = "storm.auth.simple-acl.topousers.commands";

    protected Set<String> userCommands;
    protected Set<String> admins;
    protected Set<String> supervisors;
    protected Set<String> supervisorCommands;
    protected Set<String> topoUserWhitelist;
    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    @Override
    public void prepare(Map conf) {
        try {
            users = new HashSet<String>();
            userCommands = new HashSet<String>();
            admins = new HashSet<String>();
            supervisors = new HashSet<String>();
            supervisorCommands = new HashSet<String>();
            topoUserWhitelist = new HashSet<String>();

            LOG.debug(ACL_USERS_CONF);
            if (conf.containsKey(ACL_USERS_CONF)) {
                users.addAll((Collection<String>)conf.get(ACL_USERS_CONF));
            }
            if (conf.containsKey(ACL_USERS_COMMANDS_CONF)) {
                userCommands.addAll((Collection<String>)conf.get(ACL_USERS_COMMANDS_CONF));
            }
            if (conf.containsKey(ACL_SUPERVISORS_CONF)) {
                supervisors.addAll((Collection<String>)conf.get(ACL_SUPERVISORS_CONF));
            }
            if (conf.containsKey(ACL_SUPERVISORS_COMMANDS_CONF)) {
                supervisorCommands.addAll((Collection<String>)conf.get(ACL_SUPERVISORS_COMMANDS_CONF));
            }
            if (conf.containsKey(ACL_ADMINS_CONF)) {
                admins.addAll((Collection<String>)conf.get(ACL_ADMINS_CONF));
            }
            if (conf.containsKey(ACL_TOPOUSERS_WHITELIST_CONF)) {
                topoUserWhitelist.addAll((Collection<String>)conf.get(ACL_TOPOUSERS_WHITELIST_CONF));
            }
        } catch (Exception e) {
            LOG.debug("Couldn't get all sets.", e);
        }
    }

    /**
     * permit() method is invoked for each incoming Thrift request
     * @param context request context includes info about 
     * @param operation operation name
     * @param topology_storm configuration of targeted topology 
     * @return true if the request is authorized, false if reject
     */
    @Override
    public boolean permit(ReqContext context, String operation, Map topology_conf) {

        LOG.info("[req "+ context.requestID()+ "] Access "
                 + " from: " + (context.remoteAddress() == null? "null" : context.remoteAddress().toString())
                 + (context.principal() == null? "" : (" principal:"+ context.principal()))
                 +" op:"+operation
                 + (topology_conf == null? "" : (" topoology:"+topology_conf.get(Config.TOPOLOGY_NAME))));
        
        Set topoUsers = new HashSet<String>();
        Set topoUserCommands = new HashSet<String>();
        
        if(topology_conf != null) {
            if(topology_conf.containsKey(ACL_USERS_CONF))
                topoUsers.addAll((Collection<String>)topology_conf.get(ACL_USERS_CONF));
            if(topology_conf.containsKey(ACL_USERS_COMMANDS_CONF))
                topoUserCommands.addAll((Collection<String>)topology_conf.get(ACL_USERS_COMMANDS_CONF));
        }
        
        if((topoUsers.contains(context.principal().getName()) && topoUserCommands.contains(operation) && topoUserWhitelist.contains(operation))
           || (users.contains(context.principal().getName()) && userCommands.contains(operation))
           || (supervisors.contains(context.principal().getName()) && supervisorCommands.contains(operation))
           || admins.contains(context.principal().getName()))
            return true;
       
        return false;
    }
}
