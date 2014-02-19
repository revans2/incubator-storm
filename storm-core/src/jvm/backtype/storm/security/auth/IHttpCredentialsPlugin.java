package backtype.storm.security.auth;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;

import backtype.storm.security.auth.ReqContext;

/**
 * Interface for handling credentials in an HttpServletRequest
 */
public interface IHttpCredentialsPlugin {
    /**
     * Invoked once immediately after construction
     * @param storm_conf Storm configuration
     */
    void prepare(Map storm_conf);

    /**
     * Gets the user name from the request.
     * @param req the servlet request
     * @return the authenticated user, or null if none is authenticated.
     */
    String getUserName(HttpServletRequest req);

    /**
     * Populates a given context with credentials information from an HTTP
     * request.
     * @param req the servlet request
     * @return the context
     */
    ReqContext populateContext(ReqContext context, HttpServletRequest req);
}
