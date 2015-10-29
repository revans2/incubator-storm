package com.yahoo.storm.networktopography;
import backtype.storm.networktopography.AbstractDNSToSwitchMapping;
import backtype.storm.networktopography.DNSToSwitchMapping;

import java.net.InetAddress;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class implements the {@link DNSToSwitchMapping} interface
 * By applying the subnetmask to the host IP address, this class returns
 * the rack number.
 * In case of not resolvable IP address or unknown host name,
 * /default-rack is returned.
 */
public final class YahooDNSToSwitchMapping extends AbstractDNSToSwitchMapping {
    private static int subnetMask[] = {255, 255, 255, 192};
    private static Pattern ipPattern =
            Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$");

    private Map<String, String> mappingCache = new ConcurrentHashMap<String, String>();

    @Override
    public Map<String, String> resolve(List<String> names) {

        Map <String,String> m = new HashMap<String, String>();
        if (names.isEmpty()) {
            //name list is empty, return an empty list
            return m;
        }
        Matcher match = null;
        for (String name : names) {
            // See whether it's already in the cache.
            String cachedVal = mappingCache.get(name);
            if (cachedVal != null) {
                m.put(name, cachedVal);
                continue;
            }

            match = ipPattern.matcher(name);
            if (!match.matches()){
                // this is not an IP address, we consider it is a host name
                // and try to convert it to IP address
                try{
                    InetAddress ipAddress = InetAddress.getByName(name);
                    name = ipAddress.getHostAddress();
                } catch (UnknownHostException e){
                    // host name is unknown, use DEFAULT_RACK instead.
                    // do not cache this.
                    m.put(name, DEFAULT_RACK);
                    // continue on to the next host
                    continue;
                }
                // we need to match again using the IP address,
                // so we can use matcher.group() to split IP fields later
                match = ipPattern.matcher(name);
                match.matches();
            }

            String resolvedIP = applyMask(name, match);
            m.put(name, resolvedIP);
            mappingCache.put(name, resolvedIP);
        }
        return m;
    }

    // For Yahoo, we handle the unresolved hosts by adding them to the
    // DEFAULT_RACK, so we just call the existing resolve(...) method here
    // @Override: Commented out since not all deployed versions have this method
    // in the interface
    public Map<String, String> resolveValidHosts(List<String> names)
            throws UnknownHostException {
        return this.resolve(names);
    }

    private String applyMask(String ipAddress, Matcher match) {
        int[] net = new int[4];
        StringBuilder rackip = new StringBuilder();
        rackip.append("/");
        for (int i = 0; i < 4; i++) {
            // match.group(0) refers to the entire string. subgroups start with 1.
            String ipf = match.group(i+1);
            int ipFiled = new Integer(ipf).intValue();
            if ( ipFiled < 0 || ipFiled > 256){
                // IP filed is out of range [0-255], directly return default rack.
                return DEFAULT_RACK;
            }
            net[i] = ipFiled & subnetMask[i];
            rackip.append(Integer.toString(net[i]));
            if (i != 3){
                rackip.append(".");
            }
        }
        return rackip.toString();
    }

    @Override
    public Map<String, String> getSwitchMap() {
        Map<String, String > switchMap = new HashMap<String, String>(mappingCache);
        return switchMap;
    }

    @Override
    public boolean isSingleSwitch() {
        return false;
    }

    @Override
    public String toString() {
        return "YahooDNSToSwitchMapping (" + mappingCache.size() + " mappings cached)" + dumpTopology();
    }
}
