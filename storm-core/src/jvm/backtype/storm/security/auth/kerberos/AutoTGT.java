package backtype.storm.security.auth.kerberos;

import backtype.storm.security.auth.IAutoCredentials;
import backtype.storm.security.auth.ICredentialsRenewer;
import backtype.storm.security.auth.AuthUtils;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.RefreshFailedException;
import javax.security.auth.Subject;
import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Automatically take a users TGT, and push it, and renew it in Nimbus.
 */
public class AutoTGT implements IAutoCredentials, ICredentialsRenewer {
    private static final Logger LOG = LoggerFactory.getLogger(AutoTGT.class);
    private static final float TICKET_RENEW_WINDOW = 0.80f;
    private Map conf;

    public void prepare(Map conf) {
        this.conf = conf;
    }

    private static KerberosTicket getTGT(Subject subject) {
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        for(KerberosTicket ticket: tickets) {
            KerberosPrincipal server = ticket.getServer();
            if (server.getName().equals("krbtgt/" + server.getRealm() + "@" + server.getRealm())) {
                return ticket;
            }
        }
        return null;
    } 

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        //Log the user in and get the TGT
        try {
            Configuration login_conf = AuthUtils.GetConfiguration(conf);
            ClientCallbackHandler client_callback_handler = new ClientCallbackHandler(login_conf);
        
            //login our user
            Configuration.setConfiguration(login_conf); 
            LoginContext lc = new LoginContext(AuthUtils.LOGIN_CONTEXT_CLIENT, client_callback_handler);
            try {
                lc.login();
                final Subject subject = lc.getSubject();
                KerberosTicket tgt = getTGT(subject);

                if (tgt == null) { //error
                    throw new RuntimeException("Fail to verify user principal with section \""
                            +AuthUtils.LOGIN_CONTEXT_CLIENT+"\" in login configuration file "+ login_conf);
                }

                if (!tgt.isForwardable()) {
                    throw new RuntimeException("The TGT found is not forwardable");
                }

                if (!tgt.isRenewable()) {
                    throw new RuntimeException("The TGT found is not renewable");
                }

                LOG.info("Pushing TGT for "+tgt.getClient()+" to topology.");
                saveTGT(tgt, credentials);
            } finally {
                lc.logout();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void saveTGT(KerberosTicket tgt, Map<String, String> credentials) {
        try {
            ByteArrayOutputStream bao = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bao);
            out.writeObject(tgt);
            out.flush();
            out.close();
            credentials.put("TGT", DatatypeConverter.printBase64Binary(bao.toByteArray()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static KerberosTicket getTGT(Map<String, String> credentials) {
        KerberosTicket ret = null;
        if (credentials != null && credentials.containsKey("TGT")) {
            try {
                ByteArrayInputStream bin = new ByteArrayInputStream(DatatypeConverter.parseBase64Binary(credentials.get("TGT")));
                ObjectInputStream in = new ObjectInputStream(bin);
                ret = (KerberosTicket)in.readObject();
                in.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return ret;
    }


    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        populateSubject(subject, credentials);
    }

    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        KerberosTicket tgt = getTGT(credentials);
        if (tgt != null) {
            KerberosTicket oldTGT = getTGT(subject);
            subject.getPrivateCredentials().add(tgt);
            if (oldTGT != null && !oldTGT.equals(tgt)) {
                subject.getPrivateCredentials().remove(oldTGT);
            }
            subject.getPrincipals().add(tgt.getClient());
        } else {
            LOG.info("No TGT found in credentials");
        }
    }

    private long getRefreshTime(KerberosTicket tgt) {
        long start = tgt.getStartTime().getTime();
        long end = tgt.getEndTime().getTime();
        return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
    }

    @Override
    public void renew(Map<String,String> credentials) {
        KerberosTicket tgt = getTGT(credentials);
        if (tgt != null) {
            long refreshTime = getRefreshTime(tgt);
            long now = System.currentTimeMillis();
            if (now >= refreshTime) {
                try {
                     LOG.info("Renewing TGT for "+tgt.getClient());
                    tgt.refresh();
                    saveTGT(tgt, credentials);
                } catch (RefreshFailedException e) {
                    LOG.warn("Failed to refresh TGT", e);
                }
            }
        }
    }
}
