package com.yahoo.storm.security.yca;

import static org.junit.Assert.*;

import java.net.URI;
import java.security.Principal;
import java.util.Set;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.client.HttpClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.security.auth.ReqContext;
import yjava.filter.yca.YCAFilterLogic;

public class YcaHttpCredentialsPluginTest {
    final static Logger LOG = LoggerFactory.getLogger(YcaHttpCredentialsPluginTest.class);
    static URI bogusURI;
    static HttpClient client;

    @BeforeClass
    public static void setUp() {
        client = new HttpClient();
        bogusURI = URI.create("https://0.0.0.0:8080");
    }

    @Test
    public void testGetUserNameReturnsNullWhenNoYcaCert() {
        YcaHttpCredentialsPlugin p = new YcaHttpCredentialsPlugin();
        HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
        Mockito.when(
                req.getAttribute(YCAFilterLogic.REQUEST_ATTRIBUTE_AUTHENTICATED))
                .thenReturn(Boolean.FALSE);
        Mockito.when(
                req.getAttribute(YCAFilterLogic.REQUEST_ATTRIBUTE_CLIENT_APP_ID))
                .thenReturn((Object)"alicesApplication");
        assertNull(p.getUserName(req));
    }

    @Test
    public void testGetUserNameReturnsApp() {
        HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
        Mockito.when(
                req.getAttribute(YCAFilterLogic.REQUEST_ATTRIBUTE_AUTHENTICATED))
                .thenReturn(Boolean.TRUE);
        Object expectedAppId = "alicesApplication";
        Mockito.when(
                req.getAttribute(YCAFilterLogic.REQUEST_ATTRIBUTE_CLIENT_APP_ID))
                .thenReturn(expectedAppId);
        YcaHttpCredentialsPlugin p = new YcaHttpCredentialsPlugin();
        assertEquals(expectedAppId, p.getUserName(req));
    }

    @Test
    public void testPopulateReqContex() {
        HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
        Mockito.when(
                req.getAttribute(YCAFilterLogic.REQUEST_ATTRIBUTE_AUTHENTICATED))
                .thenReturn(Boolean.TRUE);
        Object expectedAppId = "alicesApplication";
        Mockito.when(
                req.getAttribute(YCAFilterLogic.REQUEST_ATTRIBUTE_CLIENT_APP_ID))
                .thenReturn(expectedAppId);
        ReqContext context = new ReqContext(new Subject());
        YcaHttpCredentialsPlugin p = new YcaHttpCredentialsPlugin();
        context = p.populateContext(context, req);
        Set<Principal> principals = context.subject().getPrincipals();
        assertEquals(1, principals.size());
        Principal principal = (Principal)principals.toArray()[0];
        assertEquals(expectedAppId, principal.getName());
    }
}