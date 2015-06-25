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
import backtype.storm.security.auth.SingleUserPrincipal;
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

import javax.security.auth.Subject;

public class BlobStoreTest {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreTest.class);
  protected static MiniDFSCluster dfscluster = null;
  protected static Configuration hadoopConf = null;
  URI base;
  File baseFile;

  @Before
  public void init() {
    baseFile = new File("/tmp/blob-store-test-"+UUID.randomUUID());
    base = baseFile.toURI();
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

  // Overloading the assertStoreHasExactly method accomodate Subject in order to check for authorization
  public static void assertStoreHasExactly(BlobStore store, Subject who, String ... keys)
      throws IOException, KeyNotFoundException, AuthorizationException{
    Set<String> expected = new HashSet<String>(Arrays.asList(keys));
    Set<String> found = new HashSet<String>();
    Iterator<String> c = store.listKeys(who);
    while (c.hasNext()) {
      String keyName = c.next();
      found.add(keyName);
    }
    Set<String> extra = new HashSet<String>(found);
    extra.removeAll(expected);
    assertTrue("Found extra keys in the blob store "+extra, extra.isEmpty());
    Set<String> missing = new HashSet<String>(expected);
    missing.removeAll(found);
    assertTrue("Found keys missing from the blob store "+missing, missing.isEmpty());
  }

  public static void assertStoreHasExactly(BlobStore store, String ... keys)
      throws IOException, KeyNotFoundException, AuthorizationException {
    assertStoreHasExactly(store, null, keys);
  }

  // Overloading the readInt method accomodate Subject in order to check for authorization (security turned on)
  public static int readInt(BlobStore store, Subject who, String key) throws IOException, KeyNotFoundException, AuthorizationException {
    InputStream in = store.getBlob(key, who);
    try {
      return in.read();
    } finally {
      in.close();
    }
  }

  public static int readInt(BlobStore store, String key) throws IOException, KeyNotFoundException, AuthorizationException {
    return readInt(store, null, key);
  }

  public static void readAssertEquals(BlobStore store, String key, int value) throws IOException, KeyNotFoundException, AuthorizationException {
    assertEquals(value, readInt(store, key));
  }

  // Checks for assertion when we turn on security
  public void readAssertEqualsWithAuth(BlobStore store, Subject who, String key, int value) throws IOException, KeyNotFoundException, AuthorizationException {
   assertEquals(value, readInt(store, who, key));
  }

  private LocalFsBlobStore initLocalFs() {
    LocalFsBlobStore store = new LocalFsBlobStore();
    Map conf = new HashMap();
    conf.put(Config.STORM_LOCAL_DIR, baseFile.getAbsolutePath());
    conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN,"backtype.storm.security.auth.DefaultPrincipalToLocal");
    store.prepare(conf, null);
    return store;
  }

  @Test
  public void testLocalFsWithAuth() throws Exception {
    testWithAuthentication(initLocalFs());
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

  @Test
  public void testHdfsWithAuth() throws Exception {
    // use different blobstore dir so it doesn't conflict with other tests
    testWithAuthentication(initHdfs("/storm/blobstore3"));
  }

  public Subject getSubject(String name) {
    Subject subject = new Subject();
    SingleUserPrincipal user = new SingleUserPrincipal(name);
    subject.getPrincipals().add(user);
    return subject;
  }

  // Check for Blobstore with authentication
  public void testWithAuthentication(BlobStore store) throws Exception {
    // Test with a dummy test_subject for cases where subject !=null (security turned on)
    Subject who = getSubject("test_subject");
    assertStoreHasExactly(store);

    // Tests for case when subject != null (security turned on) and
    // acls for the blob are set to WORLD_EVERYTHING
    SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING);
    AtomicOutputStream out = store.createBlob("test", metadata, who);
    out.write(1);
    out.close();
    assertStoreHasExactly(store, "test");
    // Testing whether acls are set to WORLD_EVERYTHING
    assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
    readAssertEqualsWithAuth(store, who, "test", 1);

    LOG.info("Deleting test");
    store.deleteBlob("test", who);
    assertStoreHasExactly(store);

    // Tests for case when subject != null (security turned on) and
    // acls are not set for the blob (DEFAULT)
    LOG.info("Creating test again");
    metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
    out = store.createBlob("test", metadata, who);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test");
    // Testing whether acls are set to WORLD_EVERYTHING. Here the acl should not contain WORLD_EVERYTHING because
    // the subject is neither null nor empty. The ACL should however contain USER_EVERYTHING as user needs to have
    // complete access to the blob
    assertTrue("ACL does not contain WORLD_EVERYTHING", !metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
    readAssertEqualsWithAuth(store, who, "test", 2);

    LOG.info("Updating test");
    out = store.updateBlob("test", who);
    out.write(3);
    out.close();
    assertStoreHasExactly(store, "test");
    readAssertEqualsWithAuth(store, who, "test", 3);

    LOG.info("Updating test again");
    out = store.updateBlob("test", who);
    out.write(4);
    out.flush();
    LOG.info("SLEEPING");
    Thread.sleep(2);
    assertStoreHasExactly(store, "test");
    readAssertEqualsWithAuth(store, who, "test", 3);

    //Test for subject with no principals and acls set to WORLD_EVERYTHING
    who = new Subject();
    metadata = new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING);
    LOG.info("Creating test");
    out = store.createBlob("test-empty-subject-WE", metadata, who);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test-empty-subject-WE", "test");
    // Testing whether acls are set to WORLD_EVERYTHING
    assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
    readAssertEqualsWithAuth(store, who, "test-empty-subject-WE", 2);

    //Test for subject with no principals and acls set to DEFAULT
    who = new Subject();
    metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
    LOG.info("Creating other");
    out = store.createBlob("test-empty-subject-DEF", metadata, who);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test-empty-subject-DEF", "test", "test-empty-subject-WE");
    // Testing whether acls are set to WORLD_EVERYTHING
    assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
    readAssertEqualsWithAuth(store, who, "test-empty-subject-DEF", 2);

    if (store instanceof LocalFsBlobStore) {
      ((LocalFsBlobStore) store).fullCleanup(1);
    } else if (store instanceof HdfsBlobStore) {
      ((HdfsBlobStore) store).fullCleanup(1);
    } else {
      fail("Error the blobstore is of unknowntype");
    }
    try {
      out.close();
    } catch (IOException e) {
      //This is likely to happen when we try to commit something that
      // was cleaned up.  This is expected and acceptable.
    }
  }

  public void testBasic(BlobStore store) throws Exception {
    assertStoreHasExactly(store);
    LOG.info("Creating test");
    // Tests for case when subject == null (security turned off) and
    // acls for the blob are set to WORLD_EVERYTHING
    SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler
            .WORLD_EVERYTHING);
    AtomicOutputStream out = store.createBlob("test", metadata, null);
    out.write(1);
    out.close();
    assertStoreHasExactly(store, "test");
    // Testing whether acls are set to WORLD_EVERYTHING
    assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
    readAssertEquals(store, "test", 1);

    LOG.info("Deleting test");
    store.deleteBlob("test", null);
    assertStoreHasExactly(store);

    // The following tests are run for both hdfs and local store to test the
    // update blob interface
    metadata = new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING);
    LOG.info("Creating test again");
    out = store.createBlob("test", metadata, null);
    out.write(2);
    out.close();
    assertStoreHasExactly(store, "test");
    if (store instanceof LocalFsBlobStore) {
      assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
    }
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

    // Tests for case when subject == null (security turned off) and
    // acls for the blob are set to DEFAULT (Empty ACL List) only for LocalFsBlobstore
    if (store instanceof LocalFsBlobStore) {
      metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
      LOG.info("Creating test for empty acls when security is off");
      out = store.createBlob("test-empty-acls", metadata, null);
      LOG.info("metadata {}", metadata);
      out.write(2);
      out.close();
      assertStoreHasExactly(store, "test-empty-acls", "test");
      // Testing whether acls are set to WORLD_EVERYTHING, Here we are testing only for LocalFsBlobstore
      // as the HdfsBlobstore gets the subject information of the local system user and behaves as it is
      // always authenticated.
      assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.get_acl().toString().contains("OTHER"));

      LOG.info("Deleting test-empty-acls");
      store.deleteBlob("test-empty-acls", null);
    }

    if (store instanceof LocalFsBlobStore) {
      ((LocalFsBlobStore) store).fullCleanup(1);
    } else if (store instanceof HdfsBlobStore) {
      ((HdfsBlobStore) store).fullCleanup(1);
    } else {
      fail("Error the blobstore is of unknowntype");
    }
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
