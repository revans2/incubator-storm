package backtype.storm.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple, durable, atomic K/V database. *Very inefficient*, should only be used for occasional reads/writes.
 * Every read/write hits disk.
 */
public class LocalState {
    public static Logger LOG = LoggerFactory.getLogger(LocalState.class);
    private VersionedStore _vs;
    
    public LocalState(String backingDir) throws IOException {
        LOG.info("New Local State for {}", backingDir);
        _vs = new VersionedStore(backingDir);
    }
    
    public synchronized Map<Object, Object> snapshot() throws IOException {
        int attempts = 0;
        while(true) {
            String latestPath = _vs.mostRecentVersionPath();
            //LOG.info("Taking Snapshot of the state {}", latestPath);
            if(latestPath==null) return new HashMap<Object, Object>();
            try {
                return (Map<Object, Object>) Utils.deserialize(FileUtils.readFileToByteArray(new File(latestPath)));
            } catch(IOException e) {
                attempts++;
                if(attempts >= 10) {
                    throw e;
                }
            }
        }
    }
    
    public Object get(Object key) throws IOException {
        //LOG.info("get of {}", key);
        return snapshot().get(key);
    }
    
    public synchronized void put(Object key, Object val) throws IOException {
        put(key, val, true);
    }

    public synchronized void put(Object key, Object val, boolean cleanup) throws IOException {
        //LOG.info("put of {} = {}", key, val);
        Map<Object, Object> curr = snapshot();
        curr.put(key, val);
        persist(curr, cleanup);
    }

    public synchronized void remove(Object key) throws IOException {
        remove(key, true);
    }

    public synchronized void remove(Object key, boolean cleanup) throws IOException {
        //LOG.info("remove of {}", key);
        Map<Object, Object> curr = snapshot();
        curr.remove(key);
        persist(curr, cleanup);
    }

    public synchronized void cleanup(int keepVersions) throws IOException {
        _vs.cleanup(keepVersions);
    }
    
    private void persist(Map<Object, Object> val, boolean cleanup) throws IOException {
        byte[] toWrite = Utils.serialize(val);
        String newPath = _vs.createVersion();
        //LOG.info("Writing data to {}", newPath);
        FileUtils.writeByteArrayToFile(new File(newPath), toWrite);
        _vs.succeedVersion(newPath);
        if(cleanup) _vs.cleanup(4);
    }
}
