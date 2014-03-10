package backtype.storm.security.auth;

import java.security.Principal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.security.auth.ReqContext;

public class DefaultHttpCredentialsPlugin implements IHttpCredentialsPlugin {
    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultHttpCredentialsPlugin.class);

    /**
     * No-op
     * @param storm_conf Storm configuration
     */
    @Override
    public void prepare(Map storm_conf) {
        // Do nothing.
    }

    /**
     * Gets the user name from the request principal.
     * @param req the servlet request
     * @return the authenticated user, or null if none is authenticated
     */
    @Override
    public String getUserName(HttpServletRequest req) {
        Principal princ = null;
        if (req != null && (princ = req.getUserPrincipal()) != null) {
            String userName = princ.getName();
            if (userName != null && !userName.isEmpty()) {
                LOG.debug("HTTP request had user ("+userName+")");
                return userName;
            }
        }
        return null;
    }

    /**
     * Populates a given context with a new Subject derived from the
     * credentials in a servlet request.
     * @param context the context to be populated
     * @param req the servlet request
     * @return the context
     */
    @Override
    public ReqContext populateContext(ReqContext context,
            HttpServletRequest req) {
        String userName = getUserName(req);
        if (userName != null) {
            Set<SingleUserPrincipal> principals = new HashSet<SingleUserPrincipal>(1);
            principals.add(new SingleUserPrincipal(userName));
            Subject s = new Subject(true, principals, new HashSet(), new HashSet());
            context.setSubject(s);
        }
        return context;
    }
}
