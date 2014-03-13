package com.yahoo.storm.security.yca;

import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.After;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoYCATest {
    final static Logger LOG = LoggerFactory.getLogger(AutoYCATest.class);

    @After
    public void clearCache() {
        AutoYCA.clearV2Cache();
    }

    @Test
    public void testGetV2Cert() throws Exception {
        assertNull(AutoYCA.getYcaV2Cert("test.app.id"));
        AutoYCA.setV2Cert("test.app.id", "TEST");
        assertEquals("TEST", AutoYCA.getYcaV2Cert("test.app.id"));
        assertEquals("TEST", AutoYCA.getYcaV2Cert("yahoo.test.app.id"));
    }

    @Test
    public void testGetV1Cert() throws Exception {
        assertNull(AutoYCA.getYcaV1Cert("bogus.app.id"));
    }

    @Test
    public void testPopulateEmptyCreds() throws Exception {
        AutoYCA test = new AutoYCA();
        Map conf = new HashMap();
        test.prepare(conf);
        Map<String, String> creds = new HashMap<String, String>();
        test.populateCredentials(creds);
        assertTrue(creds.isEmpty());
    }

    @Test
    public void testUpdateSubject() throws Exception {
        assertNull(AutoYCA.getYcaV2Cert("test.app.id"));
        assertNull(AutoYCA.getYcaV2Cert("ignore.me"));
        AutoYCA test = new AutoYCA();
        Map conf = new HashMap();
        test.prepare(conf);
        Map<String, String> creds = new HashMap<String, String>();
        creds.put(AutoYCA.YCA_CRED_PREFIX+"yahoo.test.app.id","TEST");
        creds.put("yahoo.ignore.me","EMPTY");
        test.populateSubject(null, creds);
        assertEquals("TEST", AutoYCA.getYcaV2Cert("test.app.id"));
        assertEquals("TEST", AutoYCA.getYcaV2Cert("yahoo.test.app.id"));
        assertNull(AutoYCA.getYcaV2Cert("ignore.me"));

        creds.put(AutoYCA.YCA_CRED_PREFIX+"yahoo.test.app.id","TEST2");
        test.updateSubject(null, creds);
        assertEquals("TEST2", AutoYCA.getYcaV2Cert("test.app.id"));
        assertEquals("TEST2", AutoYCA.getYcaV2Cert("yahoo.test.app.id"));
        assertNull(AutoYCA.getYcaV2Cert("ignore.me"));
    }
}
