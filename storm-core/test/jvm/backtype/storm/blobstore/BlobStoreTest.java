package backtype.storm.blobstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import backtype.storm.generated.AccessControlType;
import backtype.storm.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.AccessControl;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.generated.ReadableBlobMeta;
import backtype.storm.generated.SettableBlobMeta;

public class BlobStoreTest {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreTest.class);
  public static final String SUPERUSER = System.getProperty("user.name");
  public static AccessControl SUPERUSERACL = null;
  protected static MiniDFSCluster dfscluster = null;
  protected static Configuration hadoopConf = null;

  URI base;
  File baseFile;

  @Before
  public void init() {
    baseFile = new File("/tmp/blob-store-test-"+UUID.randomUUID());
    base = baseFile.toURI();
    SUPERUSERACL = new AccessControl();
    SUPERUSERACL.set_name(SUPERUSER);
    SUPERUSERACL.set_access(BlobStoreAclHandler.READ|BlobStoreAclHandler.WRITE|BlobStoreAclHandler.ADMIN);
    SUPERUSERACL.set_type(AccessControlType.USER);
  }

  @After
  public void cleanup() throws IOException {
    FileUtils.deleteDirectory(baseFile);
  }

  @AfterClass
  public static void cleanupAfterClass() throws IOException {
    if (dfscluster != null) {
      dfscluster.shutdown();
    }
  }

  public static void assertStoreHasExactly(BlobStore store, String ... keys)
      throws IOException, KeyNotFoundException, AuthorizationException {
    Set<String> expected = new HashSet<String>(Arrays.asList(keys));
    Set<String> found = new HashSet<String>();
    Iterator<String> c = store.listKeys(null);
    while (c.hasNext()) {
      String keyName = c.next();
      found.add(keyName);
      assertSuperUserACLsForBlob(store, keyName);
    }
    Set<String> extra = new HashSet<String>(found);
    extra.removeAll(expected);
    assertTrue("Found extra keys in the blob store "+extra, extra.isEmpty());
    Set<String> missing = new HashSet<String>(expected);
    missing.removeAll(found);
    assertTrue("Found keys missing from the blob store "+missing, missing.isEmpty());
  }

  private static void assertSuperUserACLsForBlob(BlobStore store, String keyName)
      throws KeyNotFoundException, AuthorizationException {
    ReadableBlobMeta blobMeta = store.getBlobMeta(keyName, null);
    boolean hasSuperACL = false;
    for(AccessControl acl: blobMeta.get_settable().get_acl()) {
      if (acl.equals(SUPERUSERACL)) {
        hasSuperACL = true;
        break;
      }
    }
    assertTrue("The SuperACL " + BlobStoreAclHandler.accessControlToString(SUPERUSERACL) +
        " is not present for blob " + keyName + ".", hasSuperACL);
  }

  public static int readInt(BlobStore store, String key) throws IOException, KeyNotFoundException, AuthorizationException {
    InputStream in = store.getBlob(key, null);
    try {
      return in.read();
    } finally {
      in.close();
    }
  }
  
  public static void readAssertEquals(BlobStore store, String key, int value) throws IOException, KeyNotFoundException, AuthorizationException {
    assertEquals(value, readInt(store, key));
  }

  private LocalFsBlobStore initLocalFs() {
    LocalFsBlobStore store = new LocalFsBlobStore();
    Map conf = new HashMap();
    conf.put(Config.STORM_LOCAL_DIR, baseFile.getAbsolutePath());
    conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN,"backtype.storm.security.auth.DefaultPrincipalToLocal");
    conf.put(Config.BLOBSTORE_SUPERUSER, SUPERUSER);
    store.prepare(conf, null);
    return store;
  }

  @Test
  public void testBasicLocalFs() throws Exception {
    testBasic(initLocalFs());
  }

  @Test
  public void testMultipleLocalFs() throws Exception {
    testMultiple(initLocalFs());
  }

  private HdfsBlobStore initHdfs(String dirName) throws Exception {
    if (hadoopConf == null) {
      hadoopConf = new Configuration();
    }
    try {
      if (dfscluster == null) {
        dfscluster = new MiniDFSCluster.Builder(hadoopConf).build();
        dfscluster.waitActive();
      }
    } catch (IOException e) {
      LOG.error("error creating MiniDFSCluster");
    }
    Map conf = new HashMap();
    conf.put(Config.BLOBSTORE_DIR, dirName);
    conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN,"backtype.storm.security.auth.DefaultPrincipalToLocal");
    conf.put(Config.BLOBSTORE_SUPERUSER, SUPERUSER);
    HdfsBlobStore store = new HdfsBlobStore();
    store.prepareInternal(conf, null, hadoopConf);
    return store;
  }

  @Test
  public void testBasicHdfs() throws Exception {
    testBasic(initHdfs("/storm/blobstore1"));
  }

  @Test
  public void testMultipleHdfs() throws Exception {
    // use different blobstore dir so it doesn't conflict with other test
    testMultiple(initHdfs("/storm/blobstore2"));
  }


  public void testBasic(BlobStore store) throws Exception {

    assertStoreHasExactly(store);
    LOG.info("Creating test");
    AtomicOutputStream out = store.createBlob("test", new SettableBlobMeta(BlobStoreAclHandler
        .WORLD_EVERYTHING), null);
    out.write(1);
    out.close();
    assertStoreHasExactly(store, "test");
    readAssertEquals(store, "test", 1);

    LOG.info("Deleting test");
    store.deleteBlob("test", null);
    assertStoreHasExactly(store);

    LOG.info("Creating test again");
    out = store.createBlob("test", new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING),
        null);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test");
    readAssertEquals(store, "test", 2);

    LOG.info("Updating test");
    out = store.updateBlob("test", null);
    out.write(3);
    out.close();
    assertStoreHasExactly(store, "test");
    readAssertEquals(store, "test", 3);
    
    LOG.info("Updating test again");
    out = store.updateBlob("test", null);
    out.write(4);
    out.flush();
    LOG.info("SLEEPING");
    Thread.sleep(2);

    if (store instanceof LocalFsBlobStore) {
      ((LocalFsBlobStore) store).fullCleanup(1);
    } else if (store instanceof HdfsBlobStore) {
      ((HdfsBlobStore) store).fullCleanup(1);
    } else {
      fail("Error the blobstore is of unknowntype");
    }
    assertStoreHasExactly(store, "test");
    readAssertEquals(store, "test", 3);
    try {
      out.close();
    } catch (IOException e) {
      //This is likely to happen when we try to commit something that
      // was cleaned up.  This is expected and acceptable.
    }
  }


  public void testMultiple(BlobStore store) throws Exception {

    assertStoreHasExactly(store);
    LOG.info("Creating test");
    AtomicOutputStream out = store.createBlob("test", new SettableBlobMeta(BlobStoreAclHandler
        .WORLD_EVERYTHING), null);
    out.write(1);
    out.close();
    assertStoreHasExactly(store, "test");
    readAssertEquals(store, "test", 1);

    LOG.info("Creating other");
    out = store.createBlob("other", new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING),
        null);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test", "other");
    readAssertEquals(store, "test", 1);
    readAssertEquals(store, "other", 2);

    LOG.info("Updating other");
    out = store.updateBlob("other", null);
    out.write(5);
    out.close();
    assertStoreHasExactly(store, "test", "other");
    readAssertEquals(store, "test", 1);
    readAssertEquals(store, "other", 5);
 
    LOG.info("Deleting test");
    store.deleteBlob("test", null);
    assertStoreHasExactly(store, "other");
    readAssertEquals(store, "other", 5);

    LOG.info("Creating test again");
    out = store.createBlob("test", new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING),
        null);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test", "other");
    readAssertEquals(store, "test", 2);
    readAssertEquals(store, "other", 5);

    LOG.info("Updating test");
    out = store.updateBlob("test", null);
    out.write(3);
    out.close();
    assertStoreHasExactly(store, "test", "other");
    readAssertEquals(store, "test", 3);
    readAssertEquals(store, "other", 5);

    LOG.info("Deleting other");
    store.deleteBlob("other", null);
    assertStoreHasExactly(store, "test");
    readAssertEquals(store, "test", 3);

    LOG.info("Updating test again");
    out = store.updateBlob("test", null);
    out.write(4);
    out.flush();
    LOG.info("SLEEPING");
    Thread.sleep(2);

    if (store instanceof LocalFsBlobStore) {
      ((LocalFsBlobStore) store).fullCleanup(1);
    } else if (store instanceof HdfsBlobStore) {
      ((HdfsBlobStore) store).fullCleanup(1);
    } else {
      fail("Error the blobstore is of unknowntype");
    }    assertStoreHasExactly(store, "test");
    readAssertEquals(store, "test", 3);
    try {
      out.close();
    } catch (IOException e) {
      //This is likely to happen when we try to commit something that
      // was cleaned up.  This is expected and acceptable.
    }
  }
}
