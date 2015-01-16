package backtype.storm.localizer;

import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a resource that is localized on the supervisor.
 * A localized resource has a .current symlink to the current version file which is named
 * filename.{current version}. There is also a filename.version which contains the latest version.
 */
public class LocalizedResource {
  public static final Logger LOG = LoggerFactory.getLogger(LocalizedResource.class);

  // filesystem path to the resource
  private final String _localPath;
  private final String _versionFilePath;
  private final String _symlinkPath;
  private final String _key;
  private final boolean _uncompressed;
  // _size of the resource
  private long _size = -1;
  // queue of topologies referencing resource
  private final Set<String> _ref;
  // last access time of the resource -> increment when topology finishes using it
  private final AtomicLong _lastAccessTime = new AtomicLong(currentTime());

  public LocalizedResource(String key, String fileLoc, boolean uncompressed) {
    _ref = new HashSet<String>();
    _localPath = fileLoc;
    _versionFilePath = Utils.constructVersionFileName(fileLoc);
    _symlinkPath = Utils.constructBlobCurrentSymlinkName(fileLoc);
    _uncompressed = uncompressed;
    _key = key;
    // we trust that the file exists
    _size = Utils.getDU(new File(getFilePathWithVersion()));
    LOG.debug("size of {} is: {}", fileLoc, _size);
  }

  // create local resource and add reference
  public LocalizedResource(String key, String fileLoc, boolean uncompressed, String topo) {
    this(key, fileLoc, uncompressed);
    _ref.add(topo);
  }

  public boolean isUncompressed() {
    return _uncompressed;
  }

  public String getKey() {
    return _key;
  }

  public String getCurrentSymlinkPath() {
    return _symlinkPath;
  }

  public String getVersionFilePath() {
    return _versionFilePath;
  }

  public String getFilePathWithVersion() {
    long version = Utils.localVersionOfBlob(_localPath);
    return Utils.constructBlobWithVerionFileName(_localPath, version);
  }

  public String getFilePath() {
    return _localPath;
  }

  public void addReference(String topo) {
    _ref.add(topo);
  }

  public void removeReference(String topo) {
    if (!_ref.remove(topo)) {
      LOG.warn("Tried to remove a reference to a topology that doesn't use this resource");
    }
    setTimestamp();
  }

  // The last access time is only valid if the resource doesn't have any references.
  public long getLastAccessTime() {
    return _lastAccessTime.get();
  }

  // for testing
  protected void setSize(long size) {
    _size = size;
  }

  public long getSize() {
    return _size;
  }

  private void setTimestamp() {
    _lastAccessTime.set(currentTime());
  }

  public int getRefCount() {
    return _ref.size();
  }

  private long currentTime() {
    return System.nanoTime();
  }

}
