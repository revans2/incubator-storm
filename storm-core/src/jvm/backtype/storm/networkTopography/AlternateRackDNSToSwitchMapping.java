package backtype.storm.networkTopography;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements the {@link DNSToSwitchMapping} interface
 *    It alternates bewteen RACK1 and RACK2 for the hosts.
 */
public final class AlternateRackDNSToSwitchMapping extends CachedDNSToSwitchMapping {

    private Map<String, String> mappingCache = new ConcurrentHashMap<String, String>();

    public AlternateRackDNSToSwitchMapping() {
        super(null);
    }

    @Override
    public List<String> resolve(List<String> names) {

        List <String> m = new ArrayList<String>(names.size());
        if (names.isEmpty()) {
            //name list is empty, return an empty list
            return m;
        }

        Boolean odd = true;
        for (String name : names) {
            if (odd) {
                m.add("RACK1");
                mappingCache.put(name, "RACK1");
                odd = false;
            } else {
                m.add("RACK2");
                mappingCache.put(name, "RACK2");
                odd = true;
            }
        }
        return m;
    }

    @Override
    public String toString() {
        return "defaultRackDNSToSwitchMapping (" + mappingCache.size() + " mappings cached)";
    }
}