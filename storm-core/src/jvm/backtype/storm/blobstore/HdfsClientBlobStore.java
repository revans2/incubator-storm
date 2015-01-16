package backtype.storm.blobstore;

import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.KeyAlreadyExistsException;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.generated.ReadableBlobMeta;
import backtype.storm.generated.SettableBlobMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 *  Client to access the HDFS blobStore. At this point, this is meant to only be used by the
 *  supervisor.  Don't trust who the client says they are so pass null for all Subjects.
 */
public class HdfsClientBlobStore extends ClientBlobStore {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsClientBlobStore.class);

  private HdfsBlobStore _blobStore;
  private Map _conf;

  @Override
  public void prepare(Map conf) {
    this._conf = conf;
    _blobStore = new HdfsBlobStore();
    _blobStore.prepare(conf, null);
  }

  @Override
  public AtomicOutputStream createBlobToExtend(String key, SettableBlobMeta meta)
      throws AuthorizationException, KeyAlreadyExistsException {
      return _blobStore.createBlob(key, meta, null);
  }

  @Override
  public AtomicOutputStream updateBlob(String key)
      throws AuthorizationException, KeyNotFoundException {
    return _blobStore.updateBlob(key, null);
  }

  @Override
  public ReadableBlobMeta getBlobMeta(String key)
      throws AuthorizationException, KeyNotFoundException {
    return _blobStore.getBlobMeta(key, null);
  }

  @Override
  public void setBlobMetaToExtend(String key, SettableBlobMeta meta)
      throws AuthorizationException, KeyNotFoundException {
    _blobStore.setBlobMeta(key, meta, null);
  }

  @Override
  public void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException {
    _blobStore.deleteBlob(key, null);
  }

  @Override
  public InputStreamWithMeta getBlob(String key)
      throws AuthorizationException, KeyNotFoundException {
    return _blobStore.getBlob(key, null);
  }

  @Override
  public Iterator<String> listKeys() {
    return _blobStore.listKeys(null);
  }

  @Override
  public void watchBlob(String key, IBlobWatcher watcher)
      throws AuthorizationException {
    throw new RuntimeException("Blob watching is not implemented yet");
  }

  @Override
  public void stopWatchingBlob(String key) throws AuthorizationException {
    throw new RuntimeException("Blob watching is not implemented yet");
  }

  @Override
  public void shutdown() {
    // do nothing
  }
}
