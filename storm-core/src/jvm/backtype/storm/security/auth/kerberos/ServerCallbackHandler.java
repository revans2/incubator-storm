package backtype.storm.security.auth.kerberos;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import org.apache.zookeeper.server.auth.KerberosName;

import backtype.storm.security.auth.AuthUtils;

/**
 * SASL server side collback handler
 */
public class ServerCallbackHandler implements CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ServerCallbackHandler.class);
    private static final String SYSPROP_REMOVE_HOST = "storm.kerberos.removeHostFromPrincipal";
    private static final String SYSPROP_REMOVE_REALM = "storm.kerberos.removeRealmFromPrincipal";

    private String userName;

    public ServerCallbackHandler(Configuration configuration) throws IOException {
        if (configuration==null) return;

        AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(AuthUtils.LOGIN_CONTEXT_SERVER);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '"+AuthUtils.LOGIN_CONTEXT_SERVER+"' entry in this configuration: Server cannot start.";
            LOG.error(errorMessage);
            throw new IOException(errorMessage);
        }
    }

    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                handleNameCallback((NameCallback) callback);
            } else if (callback instanceof PasswordCallback) {
                handlePasswordCallback((PasswordCallback) callback);
            } else if (callback instanceof AuthorizeCallback) {
                handleAuthorizeCallback((AuthorizeCallback) callback);
            }
        }
    }

    private void handleNameCallback(NameCallback nc) {
        LOG.debug("handleNameCallback");
        userName = nc.getDefaultName();
        nc.setName(nc.getDefaultName());
    }

    private void handlePasswordCallback(PasswordCallback pc) {
        LOG.warn("No password found for user: " + userName);
    }

    private void handleAuthorizeCallback(AuthorizeCallback ac) {
        String authenticationID = ac.getAuthenticationID();
        LOG.debug("Successfully authenticated client: authenticationID=" + authenticationID);
        ac.setAuthorized(true);

        // canonicalize authorization id according to system properties:
        // storm.kerberos.removeRealmFromPrincipal(={true,false})
        // storm.kerberos.removeHostFromPrincipal(={true,false})
        KerberosName kerberosName = new KerberosName(authenticationID);
        try {
            StringBuilder userNameBuilder = new StringBuilder(kerberosName.getShortName());
            if (shouldAppendHost(kerberosName)) {
                userNameBuilder.append("/").append(kerberosName.getHostName());
            }
            if (shouldAppendRealm(kerberosName)) {
                userNameBuilder.append("@").append(kerberosName.getRealm());
            }
            LOG.debug("Setting authorizedID: " + userNameBuilder);
            ac.setAuthorizedID(userNameBuilder.toString());
        } catch (IOException e) {
            LOG.error("Failed to set name based on Kerberos authentication rules.", e);
        }
    }

    private boolean shouldAppendRealm(KerberosName kerberosName) {
        return !isSystemPropertyTrue(SYSPROP_REMOVE_REALM) && kerberosName.getRealm() != null;
    }

    private boolean shouldAppendHost(KerberosName kerberosName) {
        return !isSystemPropertyTrue(SYSPROP_REMOVE_HOST) && kerberosName.getHostName() != null;
    }

    private boolean isSystemPropertyTrue(String propertyName) {
        return "true".equals(System.getProperty(propertyName));
    }
}
