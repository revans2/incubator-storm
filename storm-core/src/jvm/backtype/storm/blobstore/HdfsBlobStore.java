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

import static backtype.storm.blobstore.BlobStoreAclHandler.ADMIN;
import static backtype.storm.blobstore.BlobStoreAclHandler.READ;
import static backtype.storm.blobstore.BlobStoreAclHandler.WRITE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Map;
import javax.security.auth.Subject;

import backtype.storm.utils.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.KeyAlreadyExistsException;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.generated.ReadableBlobMeta;
import backtype.storm.generated.SettableBlobMeta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Provides a HDFS file system backed blob store implementation.
 * Note that this provides an api for having HDFS be the backing store for the blobstore,
 * it is not a service/daemon.
 */
public class HdfsBlobStore extends BlobStore {
  public static final Logger LOG = LoggerFactory.getLogger(HdfsBlobStore.class);
  private static final String DATA_PREFIX = "data_";
  private static final String META_PREFIX = "meta_";
  private BlobStoreAclHandler _aclHandler;
  private HdfsBlobStoreImpl _hbs;
  private Subject _localSubject;

  /*
   * Get the subject from Hadoop so we can use it to validate the acls. There is no direct
   * interface from UserGroupInformation to get the subject, so do a doAs and get the context.
   * We could probably run everything in the doAs but for now just grab the subject.
   */
  private Subject getHadoopUser() {
    Subject subj;
    try {
      subj = UserGroupInformation.getCurrentUser().doAs(
          new PrivilegedAction<Subject>() {
            @Override
            public Subject run() {
              return Subject.getSubject(AccessController.getContext());
            }
          });
    } catch (IOException e) {
      throw new RuntimeException("Error creating subject and logging user in!", e);
    }
    return subj;
  }

  // If who is null then we want to use the user hadoop says we are.
  // Required for the supervisor to call these routines as its not
  // logged in as anyone.
  private Subject checkAndGetSubject(Subject who) {
    if (who == null) {
      return _localSubject;
    }
    return who;
  }

  @Override
  public void prepare(Map conf, String overrideBase) {
    prepareInternal(conf, overrideBase, null);
  }

  /*
   * Allow a Hadoop Configuration to be passed for testing. If it's null then the hadoop configs
   * must be in your classpath.
   */
  protected void prepareInternal(Map conf, String overrideBase, Configuration hadoopConf) {
    if (overrideBase == null) {
      overrideBase = (String)conf.get(Config.BLOBSTORE_DIR);
    }
    if (overrideBase == null) {
      throw new RuntimeException("You must specify a blobstore directory for HDFS to use!");
    }
    LOG.debug("directory is: {}", overrideBase);
    try {
      // if a HDFS keytab/principal have been supplied login, otherwise assume they are
      // logged in already or running insecure HDFS.
      if (conf.get(Config.BLOBSTORE_HDFS_PRINCIPAL) != null &&
          conf.get(Config.BLOBSTORE_HDFS_KEYTAB) != null) {
        UserGroupInformation.loginUserFromKeytab((String) conf.get(Config.BLOBSTORE_HDFS_PRINCIPAL),
            (String) conf.get(Config.BLOBSTORE_HDFS_KEYTAB));
      } else {
        if (conf.get(Config.BLOBSTORE_HDFS_PRINCIPAL) == null &&
            conf.get(Config.BLOBSTORE_HDFS_KEYTAB) != null) {
          throw new RuntimeException("You must specify an HDFS principal to go with the keytab!");

        } else {
          if (conf.get(Config.BLOBSTORE_HDFS_PRINCIPAL) != null &&
              conf.get(Config.BLOBSTORE_HDFS_KEYTAB) == null) {
            throw new RuntimeException("You must specify HDFS keytab go with the principal!");
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error logging in from keytab!", e);
    }
    Path baseDir = new Path(overrideBase, BASE_BLOBS_DIR_NAME);
    try {
      if (hadoopConf != null) {
        _hbs = new HdfsBlobStoreImpl(baseDir, conf, hadoopConf);
      } else {
        _hbs = new HdfsBlobStoreImpl(baseDir, conf);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    _localSubject = getHadoopUser();
    _aclHandler = new BlobStoreAclHandler(conf);
  }

  @Override
  public AtomicOutputStream createBlob(String key, SettableBlobMeta meta, Subject who)
      throws AuthorizationException, KeyAlreadyExistsException {
    who = checkAndGetSubject(who);
    validateKey(key);
    _aclHandler.normalizeSettableBlobMeta(meta, who, READ | WRITE | ADMIN);
    BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
    _aclHandler.validateACL(meta.get_acl(), READ | WRITE | ADMIN, who, key);
    if (_hbs.exists(DATA_PREFIX+key)) {
      throw new KeyAlreadyExistsException(key);
    }
    BlobStoreFileOutputStream mOut = null;
    try {
      mOut = new BlobStoreFileOutputStream(_hbs.write(META_PREFIX+key, true));
      mOut.write(Utils.thriftSerialize((TBase) meta));
      mOut.close();
      mOut = null;
      return new BlobStoreFileOutputStream(_hbs.write(DATA_PREFIX+key, true));
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (mOut != null) {
        try {
          mOut.cancel();
        } catch (IOException e) {
          //Ignored
        }
      }
    }
  }

  @Override
  public AtomicOutputStream updateBlob(String key, Subject who)
      throws AuthorizationException, KeyNotFoundException {
    who = checkAndGetSubject(who);
    SettableBlobMeta meta = getStoredBlobMeta(key);
    validateKey(key);
    _aclHandler.validateACL(meta.get_acl(), WRITE, who, key);
    try {
      return new BlobStoreFileOutputStream(_hbs.write(DATA_PREFIX+key, false));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private SettableBlobMeta getStoredBlobMeta(String key) throws KeyNotFoundException {
    InputStream in = null;
    try {
      BlobStoreFile pf = _hbs.read(META_PREFIX + key);
      try {
        in = pf.getInputStream();
      } catch (FileNotFoundException fnf) {
        throw new KeyNotFoundException(key);
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[2048];
      int len;
      while ((len = in.read(buffer)) > 0) {
        out.write(buffer, 0, len);
      }
      in.close();
      in = null;
      SettableBlobMeta sbm = Utils.thriftDeserialize(SettableBlobMeta.class, out.toByteArray());
      return sbm;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          //Ignored
        }
      }
    }
  }

  @Override
  public ReadableBlobMeta getBlobMeta(String key, Subject who)
      throws AuthorizationException, KeyNotFoundException {
    who = checkAndGetSubject(who);
    validateKey(key);
    SettableBlobMeta meta = getStoredBlobMeta(key);
    _aclHandler.validateUserCanReadMeta(meta.get_acl(), who, key);
    ReadableBlobMeta rbm = new ReadableBlobMeta();
    rbm.set_settable(meta);
    try {
      BlobStoreFile pf = _hbs.read(DATA_PREFIX + key);
      rbm.set_version(pf.getModTime());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return rbm;
  }

  @Override
  public void setBlobMeta(String key, SettableBlobMeta meta, Subject who)
      throws AuthorizationException, KeyNotFoundException {
    who = checkAndGetSubject(who);
    validateKey(key);
    _aclHandler.normalizeSettableBlobMeta(meta, who, ADMIN);
    BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
    SettableBlobMeta orig = getStoredBlobMeta(key);
    _aclHandler.validateACL(orig.get_acl(), ADMIN, who, key);
    BlobStoreFileOutputStream mOut = null;
    try {
      mOut = new BlobStoreFileOutputStream(_hbs.write(META_PREFIX + key, false));
      mOut.write(Utils.thriftSerialize((TBase) meta));
      mOut.close();
      mOut = null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (mOut != null) {
        try {
          mOut.cancel();
        } catch (IOException e) {
          //Ignored
        }
      }
    }
  }

  @Override
  public void deleteBlob(String key, Subject who)
      throws AuthorizationException, KeyNotFoundException {
    who = checkAndGetSubject(who);
    validateKey(key);
    SettableBlobMeta meta = getStoredBlobMeta(key);
    _aclHandler.validateACL(meta.get_acl(), WRITE, who, key);
    try {
      _hbs.deleteKey(DATA_PREFIX + key);
      _hbs.deleteKey(META_PREFIX + key);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public InputStreamWithMeta getBlob(String key, Subject who)
      throws AuthorizationException, KeyNotFoundException {
    who = checkAndGetSubject(who);
    validateKey(key);
    SettableBlobMeta meta = getStoredBlobMeta(key);
    _aclHandler.validateACL(meta.get_acl(), READ, who, key);
    try {
      return new BlobStoreFileInputStream(_hbs.read(DATA_PREFIX + key));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<String> listKeys(Subject who) {
    try {
      return new KeyTranslationIterator(_hbs.listKeys(), DATA_PREFIX);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {
    //Empty
  }

  public void fullCleanup(long age) throws IOException {
    _hbs.fullCleanup(age);
  }
}
