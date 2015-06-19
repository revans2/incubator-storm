package com.yahoo.storm.security.auth;

import static org.junit.Assert.*;

import org.apache.commons.io.IOUtils;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;

import java.io.IOException;

import yjava.byauth.jaas.LoginFailureException;

import java.util.Set;

public class YOpsDBGroupsMappingTest {

    final static Logger LOG = LoggerFactory.getLogger(YOpsDBGroupsMappingTest.class);
    private static final String OPSDB_RESULT = "/opsdb_result.json";

    OpsDBClient spyOpsDBClient = Mockito.spy(new OpsDBClient());

    class TestOpsDBGroupsMapping extends YOpsDBGroupsMapping {

        @Override
        protected OpsDBClient getOpsDBClient(Map storm_conf) {
            return spyOpsDBClient;
        }
    }

    @Test
    public void testOpsDBGetGroupNamesForUser() throws IOException, LoginFailureException {
        String user = "storm";
        String group = "ystorm_users";
        String result = IOUtils.toString(this.getClass().getResourceAsStream(OPSDB_RESULT), "UTF-8");
        OpsDBClient opsDBClient = Mockito.spy(new OpsDBClient());
        Mockito.doReturn(result).when(opsDBClient).queryUserGroups(user, false);
        Set<String> groups = opsDBClient.getGroupNamesForUser(user);
        assertEquals("number of groups doesn't match", 1, groups.size());
        assertTrue(groups.contains(group));
    }

    @Test
    public void testOpsDBGroupsMapping() throws IOException, LoginFailureException {
        String user = "storm";
        String group = "ystorm_users";
        Map conf = new HashMap();
        conf.put("storm.group.mapping.service.cache.duration.secs", "10");
        String result = IOUtils.toString(this.getClass().getResourceAsStream(OPSDB_RESULT), "UTF-8");
        YOpsDBGroupsMapping opsDBGroupsMapping = new TestOpsDBGroupsMapping();
        Mockito.doReturn(result).when(spyOpsDBClient).queryUserGroups(user, false);
        opsDBGroupsMapping.prepare(conf);
        assertEquals(1, opsDBGroupsMapping.getGroups(user).size());
        Mockito.verify(spyOpsDBClient, Mockito.times(1)).getGroupNamesForUser(user);
    }
}
