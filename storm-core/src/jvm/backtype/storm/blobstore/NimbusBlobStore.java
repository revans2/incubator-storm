package backtype.storm.blobstore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.thrift.TException;

import backtype.storm.Config;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.BeginDownloadResult;
import backtype.storm.generated.ListBlobsResult;
import backtype.storm.generated.ReadableBlobMeta;
import backtype.storm.generated.SettableBlobMeta;
import backtype.storm.generated.KeyAlreadyExistsException;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NimbusBlobStore extends ClientBlobStore {
  private static final Logger LOG = LoggerFactory.getLogger(NimbusBlobStore.class);

  public class NimbusKeyIterator implements Iterator<String> {
    private ListBlobsResult listBlobs = null;
    private int offset = 0;
    private boolean eof = false;
    
    public NimbusKeyIterator(ListBlobsResult listBlobs) {
      this.listBlobs = listBlobs;
      this.eof = (listBlobs.get_keys_size() == 0);
    }
    
    private boolean isCacheEmpty() {
      return listBlobs.get_keys_size() <= offset;
    }

    private void readMore() {
      if (!eof) {
        try {
          offset = 0;
          synchronized(client) {
            listBlobs = client.getClient().listBlobs(listBlobs.get_session());
          }
          if (listBlobs.get_keys_size() == 0) {
            eof = true;
          }
        } catch (TException e) {
          throw new RuntimeException(e);
        }
      }
    }
    
    @Override
    public synchronized boolean hasNext() {
      if (isCacheEmpty()) {
        readMore();
      }
      return !eof;
    }

    @Override
    public synchronized String next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      String ret = listBlobs.get_keys().get(offset);
      offset++;
      return ret;
    }
    
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Delete Not Supported");
    }
  }

  public class NimbusDownloadInputStream extends InputStreamWithMeta {
    private BeginDownloadResult beginBlobDownload;
    private byte[] buffer = null;
    private int offset = 0;
    private int end = 0;
    private boolean eof = false;

    public NimbusDownloadInputStream(BeginDownloadResult beginBlobDownload) {
      this.beginBlobDownload = beginBlobDownload;
    }

    @Override
    public long getVersion() throws IOException {
      return beginBlobDownload.get_version();
    }

    @Override
    public synchronized int read() throws IOException {
      if (isEmpty()) {
        readMore();
        if (eof) {
          return -1;
        }
      }
      int length = Math.min(1, available());
      if (length == 0) {
        return -1;
      }
      int ret = buffer[offset];
      offset += length;
      return ret;
    }
    
    @Override 
    public synchronized int read(byte[] b, int off, int len) throws IOException {
      if (isEmpty()) {
        readMore();
        if (eof) {
          return -1;
        }
      }
      int length = Math.min(len, available());
      System.arraycopy(buffer, offset, b, off, length);
      offset += length;
      return length;
    }
    
    private boolean isEmpty() {
      return buffer == null || offset >= end;
    }
    
    private void readMore() {
      if (!eof) {
        try {
          ByteBuffer buff;
          synchronized(client) {
            buff = client.getClient().downloadBlobChunk(beginBlobDownload.get_session());
          }
          buffer = buff.array();
          offset = buff.arrayOffset() + buff.position();
          int length = buff.remaining();
          end = offset + length;
          if (length == 0) {
            eof = true;
          }
        } catch (AuthorizationException | TException e) {
          throw new RuntimeException(e);
        }
      }
    }
    
    @Override 
    public synchronized int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
    }
    
    @Override
    public synchronized int available() {
      return buffer == null ? 0 : (end - offset);
    }
  }

  public class NimbusUploadAtomicOutputStream extends AtomicOutputStream {
    private String session;
    private int maxChunkSize = 4096;

    public NimbusUploadAtomicOutputStream(String session, int bufferSize) {
      this.session = session;
      this.maxChunkSize = bufferSize;
    }

    @Override
    public void cancel() throws IOException {
      try {
        synchronized(client) {
          client.getClient().cancelBlobUpload(session);
        }
      } catch (AuthorizationException | TException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void write(int b) throws IOException {
      try {
        synchronized(client) {
          client.getClient().uploadBlobChunk(session, ByteBuffer.wrap(new byte[] {(byte)b}));
        }
      } catch (AuthorizationException | TException e) {
        throw new RuntimeException(e);
      }
    }
    
    @Override
    public void write(byte []b) throws IOException {
      write(b, 0, b.length);
    }
    
    @Override
    public void write(byte []b, int offset, int len) throws IOException {
      try {
        int end = offset + len;
        for (int realOffset = offset; realOffset < end; realOffset += maxChunkSize) {
          int realLen = Math.min(end - realOffset, maxChunkSize);
          LOG.debug("Writing {} bytes of {} remaining",realLen,(end-realOffset));
          synchronized(client) {
            client.getClient().uploadBlobChunk(session, ByteBuffer.wrap(b, realOffset, realLen));
          }
        }
      } catch (AuthorizationException | TException e) {
        throw new RuntimeException(e);
      }
    }
    
    @Override
    public void close() throws IOException {
      try {
        synchronized(client) {
          client.getClient().finishBlobUpload(session);
        }
      } catch (AuthorizationException | TException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private NimbusClient client;
  private int bufferSize = 4096;

  @Override
  public void prepare(Map conf) {
    this.client = NimbusClient.getConfiguredClient(conf);
    if (conf != null) {
        this.bufferSize = Utils.getInt(conf.get(Config.STORM_BLOBSTORE_INPUTSTREAM_BUFFER_SIZE_BYTES), bufferSize);
    }
  }

  @Override
  protected AtomicOutputStream createBlobToExtend(String key, SettableBlobMeta meta)
      throws AuthorizationException, KeyAlreadyExistsException {
    try {
      synchronized(client) {
        return new NimbusUploadAtomicOutputStream(client.getClient().beginCreateBlob(key, meta), this.bufferSize);
      }
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public AtomicOutputStream updateBlob(String key)
      throws AuthorizationException, KeyNotFoundException {
    try {
      synchronized(client) {
        return new NimbusUploadAtomicOutputStream(client.getClient().beginUpdateBlob(key), this.bufferSize);
      }
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ReadableBlobMeta getBlobMeta(String key) throws AuthorizationException, KeyNotFoundException {
    try {
      synchronized(client) {
        return client.getClient().getBlobMeta(key);
      }
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void setBlobMetaToExtend(String key, SettableBlobMeta meta)
      throws AuthorizationException, KeyNotFoundException {
    try {
      synchronized(client) {
        client.getClient().setBlobMeta(key, meta);
      }
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException {
    try {
      synchronized(client) {
        client.getClient().deleteBlob(key);
      }
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public InputStreamWithMeta getBlob(String key) throws AuthorizationException, KeyNotFoundException {
    try {
      synchronized(client) {
        return new NimbusDownloadInputStream(client.getClient().beginBlobDownload(key));
      }
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<String> listKeys() {
    try {
      synchronized(client) {
        return new NimbusKeyIterator(client.getClient().listBlobs(""));
      }
    } catch (TException e) {
      throw new RuntimeException(e);
    }
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
  protected void finalize() {
    shutdown();
  }
  
  @Override
  public void shutdown() {
    if (client != null) {
      client.close();
      client = null;
    }
  }
}
