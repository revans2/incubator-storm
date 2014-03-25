package backtype.storm.security.auth.authorizer;

import java.lang.reflect.Field;
import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.security.auth.ReqContext;
import backtype.storm.security.auth.authorizer.DRPCAuthorizerBase;
import backtype.storm.security.auth.AuthUtils;
import backtype.storm.security.auth.IPrincipalToLocal;
import backtype.storm.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DRPCSimpleACLAuthorizer extends DRPCAuthorizerBase {
    public static Logger LOG =
        LoggerFactory.getLogger(DRPCSimpleACLAuthorizer.class);

    public static final String CLIENT_USERS_KEY = "client.users";
    public static final String INVOCATION_USER_KEY = "invocation.user";
    public static final String FUNCTION_KEY = "function.name";

    protected String _aclFileName = "";
    protected IPrincipalToLocal _ptol;
    protected boolean _permitWhenMissingFunctionEntry = false;

    protected class AclFunctionEntry {
        final public Set<String> clientUsers;
        final public String invocationUser;
        public AclFunctionEntry(Collection<String> clientUsers,
                String invocationUser) {
            this.clientUsers = (clientUsers != null) ?
                new HashSet<String>(clientUsers) : new HashSet<String>();
            this.invocationUser = invocationUser;
        }
    }

    private Map<String,AclFunctionEntry> _acl =
        new HashMap<String,AclFunctionEntry>();

    protected void readAclFromConfig() {
        _acl.clear();
        Map conf = Utils.findAndReadConfigFile(_aclFileName);
        if (conf.containsKey(Config.DRPC_AUTHORIZER_ACL)) {
            _acl.clear();
            Map<String,Map<String,?>> confAcl =
                (Map<String,Map<String,?>>)
                conf.get(Config.DRPC_AUTHORIZER_ACL);

            for (String function : confAcl.keySet()) {
                Map<String,?> val = confAcl.get(function);
                Collection<String> clientUsers =
                    val.containsKey(CLIENT_USERS_KEY) ?
                    (Collection<String>) val.get(CLIENT_USERS_KEY) : null;
                String invocationUser =
                    val.containsKey(INVOCATION_USER_KEY) ?
                    (String) val.get(INVOCATION_USER_KEY) : null;
                _acl.put(function,
                        new AclFunctionEntry(clientUsers, invocationUser));
            }
        } else if (!_permitWhenMissingFunctionEntry) {
            LOG.warn("Requiring explicit ACL entries, but none given. " +
                    "Therefore, all operiations will be denied.");
        } 
       
    }

    @Override
    public void prepare(Map conf) {
        _acl.clear();
        Boolean isStrict = 
                (Boolean) conf.get(Config.DRPC_AUTHORIZER_ACL_STRICT);
        _permitWhenMissingFunctionEntry = 
                (isStrict != null && !isStrict) ? true : false;
        _aclFileName = (String) conf.get(Config.DRPC_AUTHORIZER_ACL_FILENAME);
        _ptol = AuthUtils.GetPrincipalToLocalPlugin(conf);
    }

    private String getUserFromContext(ReqContext context) {
        if (context != null) {
            Principal princ = context.principal();
            if (princ != null) {
                return princ.getName();
            }
        }
        return null;
    }

    private String getLocalUserFromContext(ReqContext context) {
        if (context != null) {
            return _ptol.toLocal(context.principal());
        }
        return null;
    }

    protected boolean permitClientOrInvocationRequest(ReqContext context, Map params,
            String fieldName) {
        readAclFromConfig();
        String function = (String) params.get(FUNCTION_KEY);
        if (function != null && ! function.isEmpty()) {
            AclFunctionEntry entry = _acl.get(function);
            if (entry == null && _permitWhenMissingFunctionEntry) {
                return true;
            }
            if (entry != null) {
                Object value;
                try {
                    Field field = AclFunctionEntry.class.getDeclaredField(fieldName);
                    value = field.get(entry);
                } catch (Exception ex) {
                    LOG.warn("Caught Exception while accessing ACL", ex);
                    return false;
                }
                String principal = getUserFromContext(context);
                String user = getLocalUserFromContext(context);
                if (value == null) {
                    LOG.warn("Configuration for function '"+function+"' is "+
                            "invalid: it should have both an invocation user "+
                            "and a list of client users defined.");
                } else if (value instanceof Set && 
                        (((Set<String>)value).contains(principal) ||
                        ((Set<String>)value).contains(user))) {
                    return true;
                } else if (value instanceof String && 
                        (value.equals(principal) ||
                         value.equals(user))) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    protected boolean permitClientRequest(ReqContext context, String operation,
            Map params) {
        return permitClientOrInvocationRequest(context, params, "clientUsers");
    }

    @Override
    protected boolean permitInvocationRequest(ReqContext context, String operation,
            Map params) {
        return permitClientOrInvocationRequest(context, params, "invocationUser");
    }
}
