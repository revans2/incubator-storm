package com.yahoo.storm.security.auth;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.methods.HttpGet;
import org.apache.commons.io.IOUtils;

import yjava.byauth.jaas.HttpClientBouncerAuth;
import yjava.byauth.jaas.LoginFailureException;
import yjava.security.ysecure.KeyDBException;
import yjava.security.ysecure.YCR;
import yjava.security.ysecure.YCRException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Get all OpsDB user groups that a user belongs to. Documentation: http://devel.corp.yahoo.com/opsdb/api/api_v4_usergroup.html#user-group-find
 */
public class OpsDBClient {

    private static final Logger LOG = LoggerFactory.getLogger(OpsDBClient.class);
    private static final String GH_BOUNCER_LOGIN_URL = "https://gh.bouncer.login.yahoo.com/login/";
    private static final String OPSDB_END_POINT = "http://api.opsdb.ops.yahoo.com:4080/V4/UserGroups/find";
    private static final String QUERY_USERNAME = "username";
    private static final String QUERY_TYPE = "type";
    private static final String OUTPUT_FORMAT = "output=json";
    private static final String NO_PAGINATION = "without_pagination=1";
    private static final int CONNECT_TIMEOUT = 5000;
    private static final int SOCKET_TIMEOUT = 10000;
    private static final int CONNECTION_REQUEST_TIMEOUT = 5000;

    private String headlessYBYCookie;
    private String bouncerHeadlessUser;
    private String bouncerHeadlessPasswordKey;

    public OpsDBClient(String bouncerHeadlessUser, String bouncerHeadlessPasswordKey) {
        this.bouncerHeadlessUser = bouncerHeadlessUser;
        this.bouncerHeadlessPasswordKey = bouncerHeadlessPasswordKey;
        init();
    }

    public OpsDBClient() {
    }

    private void init() {
        try {
            headlessYBYCookie = getHeadlessYBYCookie(bouncerHeadlessUser, bouncerHeadlessPasswordKey);
        } catch (IOException | LoginFailureException ignore) {
        }

    }

    public String queryUserGroups(String userName, boolean isRetry) throws IOException, LoginFailureException {
        int status = 0;
        String response = null;

        if (headlessYBYCookie == null) {
            headlessYBYCookie = getHeadlessYBYCookie(bouncerHeadlessUser, bouncerHeadlessPasswordKey);
        }

        String url = composeQueryUrl(userName);

        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(CONNECT_TIMEOUT)
            .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT)
            .setSocketTimeout(SOCKET_TIMEOUT)
            .setRedirectsEnabled(false)  // Disable redirects
            .build();

        try (
            CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build();
        ) {
            HttpGet httpGet = new HttpGet(url);
            httpGet.addHeader("Cookie", headlessYBYCookie.trim());

            try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
                status = httpResponse.getStatusLine().getStatusCode();
                HttpEntity entity = httpResponse.getEntity();
                response = entity != null ? IOUtils.toString(entity.getContent())
                                          : null;
            } catch (IOException e) {
                LOG.warn("IOException while querying OpsDB url: {} {}", url, e.toString());
            }
        }

        if (status != 200) {
            if (!isRetry) {
                // renew cookie and retry
                headlessYBYCookie = getHeadlessYBYCookie(bouncerHeadlessUser, bouncerHeadlessPasswordKey);
                return queryUserGroups(userName, true);
            }
        }

        return response;
    }

    public Set<String> getGroupNamesForUser(final String userName) {
        Set<String> groups = new HashSet<String>();
        try {
            String response = queryUserGroups(userName, false);

            try {
                JSONParser jsonParser = new JSONParser();
                JSONObject jsonObject = (JSONObject) jsonParser.parse(response);
                JSONArray results = (JSONArray) jsonObject.get("result");

                Iterator it = results.iterator();

                while (it.hasNext()) {
                    JSONObject group = (JSONObject) it.next();
                    groups.add((String) group.get("name"));
                }
            } catch (ParseException e) {
                LOG.error("error parsing OpsDB api response: {}", e.toString());
            }
        } catch (IOException | LoginFailureException e) {
            LOG.error("error querying OpsDB api", e);
        }

        return groups;
    }

    private String composeQueryUrl(final String userName) {
        return OPSDB_END_POINT + "?" +
               QUERY_USERNAME + "=" + userName +
               "&" + QUERY_TYPE + "=" + "opsdb" +
               "&" + OUTPUT_FORMAT +
               "&" + NO_PAGINATION;
    }

    private static String getSecretFor(final String key) {
        try {
            YCR ykkClient = YCR.createYCR(YCR.NATIVE_YCR);
            return ykkClient.getKey(key);
        } catch (YCRException | KeyDBException e) {
            LOG.error("unable to fetch ykeykey value for key: {} error: {}", key, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static String getHeadlessYBYCookie(final String bouncerHeadlessUser,
                                               final String bouncerHeadlessPasswordKey)
        throws IOException, LoginFailureException {
        String bouncerHeadlessPassword = getSecretFor(bouncerHeadlessPasswordKey);
        try {
            HttpClientBouncerAuth auth = new HttpClientBouncerAuth();
            return auth.authenticate2(GH_BOUNCER_LOGIN_URL, bouncerHeadlessUser, bouncerHeadlessPassword.toCharArray());
        } catch (Exception e) {
            LOG.error("error logging into bouncer as user: {}", bouncerHeadlessUser);
            throw e;
        }
    }
}