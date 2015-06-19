package com.yahoo.storm.security.auth;

import backtype.storm.security.auth.IGroupMappingServiceProvider;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.Config;
import backtype.storm.utils.Utils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User groups mapping provider plugin based on OpsDB user groups.
 */
public class YOpsDBGroupsMapping implements IGroupMappingServiceProvider {

    private static final Logger LOG = LoggerFactory.getLogger(YOpsDBGroupsMapping.class);
    private TimeCacheMap<String, Set<String>> cachedGroups;
    private OpsDBClient opsDBClient;

    /**
     * Invoked once immediately after construction
     *
     * @param storm_conf Storm configuration
     */
    @Override
    public void prepare(Map storm_conf) {
        int timeout = Utils.getInt(storm_conf.get(Config.STORM_GROUP_MAPPING_SERVICE_CACHE_DURATION_SECS));
        cachedGroups = new TimeCacheMap<String, Set<String>>(timeout);
        opsDBClient = getOpsDBClient(storm_conf);
    }

    protected OpsDBClient getOpsDBClient(Map storm_conf) {
        Map<String, String> params = (Map) storm_conf.get(Config.STORM_GROUP_MAPPING_SERVICE_PARAMS);
        String bouncerHeadlessUser = params.get("bouncer_user");
        String bouncerHeadlessUserPasswordKey = params.get("bouncer_pwd_key");
        return new OpsDBClient(bouncerHeadlessUser, bouncerHeadlessUserPasswordKey);
    }

    /**
     * Returns list of groups for a user
     *
     * @param user get groups for this user
     * @return list of groups for a given user
     */
    @Override
    public Set<String> getGroups(String user) throws IOException {
        if (cachedGroups.containsKey(user)) {
            return cachedGroups.get(user);
        }
        Set<String> groups = opsDBClient.getGroupNamesForUser(user);
        if (!groups.isEmpty()) {
            cachedGroups.put(user, groups);
        }
        return groups;
    }
}