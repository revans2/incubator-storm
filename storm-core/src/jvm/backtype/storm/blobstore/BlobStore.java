/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.blobstore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.generated.KeyAlreadyExistsException;
import backtype.storm.generated.ReadableBlobMeta;
import backtype.storm.generated.SettableBlobMeta;

/**
 * Provides a way to store blobs that can be downloaded.
 * Blobs must be able to be uploaded and listed from Nimbus,
 * and downloaded from the Supervisors.
 *
 * In the future it would be nice to have blobs uploaded
 * directly by the end users.
 *
 * ACL checking must take place against the provided subject.
 * If the blob store does not support Security it must validate
 * that all ACLs set are always WORLD, everything.
 */
public abstract class BlobStore implements Shutdownable {
  public static final Logger LOG = LoggerFactory.getLogger(BlobStore.class);
  private static final Pattern KEY_PATTERN = Pattern.compile("^[\\w \\t\\.:_-]+$");
  protected static final String BASE_BLOBS_DIR_NAME = "blobs";

  public abstract void prepare(Map conf, String baseDir);
  public abstract AtomicOutputStream createBlob(String key, SettableBlobMeta meta, Subject who) throws AuthorizationException, KeyAlreadyExistsException;
  public abstract AtomicOutputStream updateBlob(String key, Subject who) throws AuthorizationException, KeyNotFoundException;
  public abstract ReadableBlobMeta getBlobMeta(String key, Subject who) throws AuthorizationException, KeyNotFoundException;
  public abstract void setBlobMeta(String key, SettableBlobMeta meta, Subject who) throws AuthorizationException, KeyNotFoundException;
  public abstract void deleteBlob(String key, Subject who) throws AuthorizationException, KeyNotFoundException;
  public abstract InputStreamWithMeta getBlob(String key, Subject who) throws AuthorizationException, KeyNotFoundException;
  public abstract Iterator<String> listKeys(Subject who);
  
  public <R> Set<R> filterAndListKeys(KeyFilter<R> filter, Subject who) {
    Set<R> ret = new HashSet<R>();
    Iterator<String> keys = listKeys(who);
    while (keys.hasNext()) {
      String key = keys.next();
      R filtered = filter.filter(key);
      if (filtered != null) {
        ret.add(filtered);
      }
    }
    return ret;
  }

  public static final void validateKey(String key) throws AuthorizationException {
    if (key == null || key.isEmpty() || "..".equals(key) || ".".equals(key) || !KEY_PATTERN.matcher(key).matches()) {
      LOG.error("'{}' does not appear to be valid {}", key, KEY_PATTERN);
      throw new AuthorizationException(key+" does not appear to be a valid blob key");
    }
  }

  public void createBlob(String key, byte [] data, SettableBlobMeta meta, Subject who) throws AuthorizationException, KeyAlreadyExistsException, IOException {
    AtomicOutputStream out = null;
    try {
      out = createBlob(key, meta, who);
      out.write(data);
      out.close();
      out = null;
    } finally {
      if (out != null) {
        out.cancel();
      }
    }
  }
  
  public void createBlob(String key, InputStream in, SettableBlobMeta meta, Subject who) throws AuthorizationException, KeyAlreadyExistsException, IOException {
    AtomicOutputStream out = null;
    try {
      out = createBlob(key, meta, who);
      byte[] buffer = new byte[2048];
      int len = 0;
      while ((len = in.read(buffer)) > 0) {
        out.write(buffer, 0, len);
      }
      out.close();
    } catch (AuthorizationException | IOException | RuntimeException e) {
      out.cancel();
    } finally {
      in.close();
    }
  }
  
  public void readBlobTo(String key, OutputStream out, Subject who) throws IOException, KeyNotFoundException, AuthorizationException {
    InputStreamWithMeta in = getBlob(key, who);
    if (in == null) {
      throw new IOException("Could not find " + key);
    }
    byte[] buffer = new byte[2048];
    int len = 0;
    try{
      while ((len = in.read(buffer)) > 0) {
        out.write(buffer, 0, len);
      }
    } finally {
      in.close();
      out.flush();
    }
  }
  
  public byte[] readBlob(String key, Subject who) throws IOException, KeyNotFoundException, AuthorizationException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    readBlobTo(key, out, who);
    return out.toByteArray();
  }

  protected class BlobStoreFileOutputStream extends AtomicOutputStream {
    private BlobStoreFile part;
    private OutputStream out;

    public BlobStoreFileOutputStream(BlobStoreFile part) throws IOException {
      this.part = part;
      this.out = part.getOutputStream();
    }

    @Override
    public void close() throws IOException {
      try {
        //close means commit
        out.close();
        part.commit();
      } catch (IOException | RuntimeException e) {
        cancel();
        throw e;
      }
    }

    @Override
    public void cancel() throws IOException {
      try {
        out.close();
      } finally {
        part.cancel();
      }
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte []b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte []b, int offset, int len) throws IOException {
      out.write(b, offset, len);
    }
  }

  protected class BlobStoreFileInputStream extends InputStreamWithMeta {
    private BlobStoreFile part;
    private InputStream in;

    public BlobStoreFileInputStream(BlobStoreFile part) throws IOException {
      this.part = part;
      this.in = part.getInputStream();
    }

    @Override
    public long getVersion() throws IOException {
      return part.getModTime();
    }

    @Override
    public int read() throws IOException {
      return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, len);
    }

    @Override
    public int read(byte[] b) throws IOException {
      return in.read(b);
    }

    @Override
    public int available() throws IOException {
      return in.available();
    }
  }

  public static class KeyTranslationIterator implements Iterator<String> {
    private Iterator<String> it = null;
    private String next = null;
    private String prefix = null;

    public KeyTranslationIterator(Iterator<String> it, String prefix) throws IOException {
      this.it = it;
      this.prefix = prefix;
      primeNext();
    }

    private void primeNext() {
      next = null;
      while (it.hasNext()) {
        String tmp = it.next();
        if (tmp.startsWith(prefix)) {
          next = tmp.substring(prefix.length());
          return;
        }
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public String next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      String current = next;
      primeNext();
      return current;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Delete Not Supported");
    }
  }
}
