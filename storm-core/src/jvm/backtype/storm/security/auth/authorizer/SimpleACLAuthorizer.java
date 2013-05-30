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
    public static String WHITELIST_USERS_COMMANDS_CONF = "storm.auth.simple-white-list.users.commands";
    public static String WHITELIST_SUPERVISORS_CONF = "supervisor.supervisors";
    public static String WHITELIST_SUPERVISORS_COMMANDS_CONF = "supervisor.supervisors.commands";
    public static String WHITELIST_ADMINS_CONF = "storm.auth.simple-white-list.admins";
    protected Set<String> userCommands;
    protected Set<String> admins;
    protected Set<String> supervisors;
    protected Set<String> supervisorCommands;

    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    @Override
    public void prepare(Map conf) {
        try {
            LOG.debug("Preparing SimpleACLAuthorizer ACL lists.");
            users = new HashSet<String>();
            userCommands = new HashSet<String>();
            admins = new HashSet<String>();
            supervisors = new HashSet<String>();
            supervisorCommands = new HashSet<String>();

            LOG.debug(WHITELIST_USERS_CONF);
            if (conf.containsKey(WHITELIST_USERS_CONF)) {
                LOG.debug("Adding users." + conf.get(WHITELIST_USERS_CONF));
                users.addAll((Collection<String>)conf.get(WHITELIST_USERS_CONF));
            }
            else
                LOG.debug(conf.toString());
            if (conf.containsKey(WHITELIST_USERS_COMMANDS_CONF)) {
                LOG.debug("Adding user commands." + conf.get(WHITELIST_USERS_COMMANDS_CONF));
                userCommands.addAll((Collection<String>)conf.get(WHITELIST_USERS_COMMANDS_CONF));
            }
            if (conf.containsKey(WHITELIST_SUPERVISORS_CONF)) {
                LOG.debug("Adding supervisors." + conf.get(WHITELIST_SUPERVISORS_CONF));
                supervisors.addAll((Collection<String>)conf.get(WHITELIST_SUPERVISORS_CONF));
            }
            if (conf.containsKey(WHITELIST_SUPERVISORS_COMMANDS_CONF)) {
                LOG.debug("Adding supervisor commands." + conf.get(WHITELIST_SUPERVISORS_COMMANDS_CONF));
                supervisorCommands.addAll((Collection<String>)conf.get(WHITELIST_SUPERVISORS_COMMANDS_CONF));
            }
            if (conf.containsKey(WHITELIST_ADMINS_CONF)) {
                LOG.debug("Adding admins." + conf.get(WHITELIST_ADMINS_CONF));
                admins.addAll((Collection<String>)conf.get(WHITELIST_ADMINS_CONF));
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
        if((users.contains(context.principal().getName()) && userCommands.contains(operation))
           || (supervisors.contains(context.principal().getName()) && supervisorCommands.contains(operation))
           || admins.contains(context.principal().getName()))
            return true;
        
        LOG.info("[req "+ context.requestID()+ "] Access Denied."
                 + "\nUser: " + context.principal().getName()
                 + "\nOperation: " + operation
                 + "\nAdmins: " + admins.toString()
                 + "\nUsers: " + users.toString()
                 + "\nSupervisors: " + supervisors.toString()
                 + "\nUser Commands: " + userCommands.toString()
                 + "\nSupervisor Commands: " + supervisorCommands.toString());
        return false;
    }
}
