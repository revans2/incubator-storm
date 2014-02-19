package backtype.storm.security.auth.kerberos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;


/**
 * Custom LoginModule to enable Auto Login based on cached ticket
 */
public class AutoTGTKrb5LoginModule implements LoginModule {
    private static final Logger LOG = LoggerFactory.getLogger(AutoTGTKrb5LoginModule.class);

    // initial state
    private Subject subject;

    protected KerberosTicket kerbTicket = null;

    public void initialize(Subject subject,
                           CallbackHandler callbackHandler,
                           Map<String, ?> sharedState,
                           Map<String, ?> options) {

        this.subject = subject;
    }

    public boolean login() throws LoginException {
        LOG.debug("Acquire TGT from Cache");
        getKerbTicketFromCache();
        if (kerbTicket != null) {
            return true;
        } else {
            throw new LoginException("Authentication failed, the TGT not found.");
        }
    }

    protected void getKerbTicketFromCache() {
        kerbTicket = AutoTGT.kerbTicket.get();
    }

    protected Principal getKerbTicketClient() {
        if (kerbTicket != null) {
            return kerbTicket.getClient();
        }
        return null;
    }

    public boolean commit() throws LoginException {
        if (isSucceeded() == false) {
            return false;
        }
        if (subject == null || subject.isReadOnly()) {
            kerbTicket = null;
            throw new LoginException("Authentication failed because the Subject is invalid.");
        }
        // Let us add the kerbClientPrinc and kerbTicket
        subject.getPrivateCredentials().add(kerbTicket);
        subject.getPrincipals().add(getKerbTicketClient());
        LOG.debug("Commit Succeeded.");
        return true;
    }

    public boolean abort() throws LoginException {
        if (isSucceeded() == false) {
            return false;
        } else {
            return logout();
        }
    }

    public boolean logout() throws LoginException {
        if (subject != null && !subject.isReadOnly() && kerbTicket != null) {
            subject.getPrincipals().remove(kerbTicket.getClient());
            subject.getPrivateCredentials().remove(kerbTicket);
        }
        kerbTicket = null;
        return true;
    }

    private boolean isSucceeded() {
        return kerbTicket != null;
    }
}
