package com.yahoo.storm.security.yca;

import javax.servlet.http.HttpServletRequest;

import yjava.filter.yca.YCAFilterLogic;
import backtype.storm.security.auth.DefaultHttpCredentialsPlugin;

/**
 * Plugin for handling YCA credentials in an HttpServletRequest
 */
public class YcaHttpCredentialsPlugin extends DefaultHttpCredentialsPlugin {
    /**
     * Gets the user name from the request.
     * @param req the servlet request
     * @return the authenticated user, or null if none is authenticated.
     */
    @Override
    public String getUserName(HttpServletRequest req) {
        if (req.getAttribute(YCAFilterLogic.REQUEST_ATTRIBUTE_AUTHENTICATED) == Boolean.TRUE) {
            Object id = req.getAttribute(YCAFilterLogic.REQUEST_ATTRIBUTE_CLIENT_APP_ID);
            if (id != null && id instanceof String) {
                return id.toString();
            }
        }
        return null;
    }
}
