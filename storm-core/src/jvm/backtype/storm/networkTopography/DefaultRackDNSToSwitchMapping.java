package backtype.storm.networkTopography;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements the {@link DNSToSwitchMapping} interface
 *    It returns the DEFAULT_RACK for every host.
 */
public final class DefaultRackDNSToSwitchMapping extends CachedDNSToSwitchMapping {

    private Map<String, String> mappingCache = new ConcurrentHashMap<String, String>();

    public DefaultRackDNSToSwitchMapping() {
        super(null);
    }

    @Override
    public List<String> resolve(List<String> names) {

        List <String> m = new ArrayList<String>(names.size());
        if (names.isEmpty()) {
            //name list is empty, return an empty list
            return m;
        }
        for (String name : names) {
            m.add(DEFAULT_RACK);
            mappingCache.put(name, DEFAULT_RACK);
        }
        return m;
    }

    @Override
    public String toString() {
        return "defaultRackDNSToSwitchMapping (" + mappingCache.size() + " mappings cached)";
    }
}