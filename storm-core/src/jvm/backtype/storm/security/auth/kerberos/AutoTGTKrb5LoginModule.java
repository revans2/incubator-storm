package backtype.storm.security.auth.kerberos;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.util.Map;
import java.io.*;
import java.text.MessageFormat;
import java.util.*;

import javax.security.auth.*;
import javax.security.auth.kerberos.*;
import javax.security.auth.callback.*;
import javax.security.auth.login.*;
import javax.security.auth.spi.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Custom LoginModule to enable Auto Login based on cached ticket
 */
public class AutoTGTKrb5LoginModule implements LoginModule {
    private static final Logger LOG = LoggerFactory.getLogger(AutoTGTKrb5LoginModule.class);

    // initial state
    private Subject subject;
    private CallbackHandler callbackHandler;
    private Map sharedState;
    private Map<String, ?> options;

    // the authentication status
    private boolean succeeded = false;
    private boolean commitSucceeded = false;

    private KerberosPrincipal kerbClientPrinc = null;
    private KerberosTicket kerbTicket = null;

    public void initialize(Subject subject,
                           CallbackHandler callbackHandler,
                           Map<String, ?> sharedState,
                           Map<String, ?> options) {

        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.sharedState = sharedState;
        this.options = options;

    }

    public boolean login() throws LoginException {
        LOG.debug("Acquire TGT from Cache");
        kerbTicket = AutoTGT.kerbTicket.get();

        if (kerbTicket != null) {
            if (kerbClientPrinc == null) {
                kerbClientPrinc = kerbTicket.getClient();
            }
            succeeded = true;
            return true;
        } else {
            LoginException loginException = new LoginException("The TGT not found.");
            LOG.debug("Authentication failed.", loginException);
            succeeded = false;
            throw loginException;
        }
    }

    public boolean commit() throws LoginException {
        if (succeeded == false) {
            return false;
        } else {

            if (subject.isReadOnly()) {
                kerbTicket = null;
                kerbClientPrinc = null;
                throw new LoginException("Subject is Readonly");
            }

            Set<Object> privCredSet = subject.getPrivateCredentials();
            Set<java.security.Principal> princSet = subject.getPrincipals();

            // Let us add the kerbClientPrinc and kerbTicket
            if (!princSet.contains(kerbClientPrinc)) {
                princSet.add(kerbClientPrinc);
            }

            // add the TGT
            if (kerbTicket != null) {
                if (!privCredSet.contains(kerbTicket))
                    privCredSet.add(kerbTicket);
            }
        }
        commitSucceeded = true;
        LOG.debug("Commit Succeeded \n");
        return true;
    }

    public boolean abort() throws LoginException {
        if (succeeded == false) {
            return false;
        } else if (succeeded == true && commitSucceeded == false) {
            succeeded = false;
            kerbClientPrinc = null;
            kerbTicket = null;
        } else {
            logout();
        }
        return true;
    }

    public boolean logout() throws LoginException {
        subject.getPrincipals().remove(kerbClientPrinc);

        Set<Object> privCredSet = subject.getPrivateCredentials();
        if (privCredSet.contains(kerbTicket))
            privCredSet.remove(kerbTicket);
        succeeded = false;
        commitSucceeded = false;
        kerbClientPrinc = null;
        kerbTicket = null;
        return true;
    }

}
