package backtype.storm.blobstore;

import backtype.storm.Config;
import backtype.storm.generated.AccessControl;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.KeyAlreadyExistsException;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.generated.ReadableBlobMeta;
import backtype.storm.generated.SettableBlobMeta;
import backtype.storm.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ClientBlobStoreTest {

  private ClientBlobStore client;
  public class TestClientBlobStore extends ClientBlobStore {

    private Map<String, SettableBlobMeta> allBlobs;
    @Override
    public void prepare(Map conf) {
      this.conf = conf;
      allBlobs = new HashMap<String, SettableBlobMeta>();
    }

    @Override
    protected AtomicOutputStream createBlobToExtend(String key, SettableBlobMeta meta) throws AuthorizationException, KeyAlreadyExistsException {
      allBlobs.put(key, meta);
      return null;
    }

    @Override
    public AtomicOutputStream updateBlob(String key) throws AuthorizationException, KeyNotFoundException {
      return null;
    }

    @Override
    public ReadableBlobMeta getBlobMeta(String key) throws AuthorizationException, KeyNotFoundException {
      ReadableBlobMeta reableMeta = null;
      if (allBlobs.containsKey(key)) {
        reableMeta = new ReadableBlobMeta();
        reableMeta.set_settable(allBlobs.get(key));
      }
      return reableMeta;
    }

    @Override
    protected void setBlobMetaToExtend(String key, SettableBlobMeta meta) throws AuthorizationException, KeyNotFoundException {
    }

    @Override
    public void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException {
    }

    @Override
    public InputStreamWithMeta getBlob(String key) throws AuthorizationException, KeyNotFoundException {
      return null;
    }

    @Override
    public Iterator<String> listKeys() {
      return null;
    }

    @Override
    public void watchBlob(String key, IBlobWatcher watcher) throws AuthorizationException {
    }

    @Override
    public void stopWatchingBlob(String key) throws AuthorizationException {
    }

    @Override
    public void shutdown() {
    }
  }

  @Before
  public void setUp() throws Exception {

    client = new TestClientBlobStore();
    Map conf = new HashMap<String,String>();
    client.prepare(conf);

  }

  @After
  public void tearDown() throws Exception {
    client = null;
  }

  @Test(expected=AuthorizationException.class)
  public void testDuplicateACLsForCreate() throws Exception {
    SettableBlobMeta meta = new SettableBlobMeta();
    AccessControl submitterAcl = BlobStoreAclHandler.parseAccessControl("u:tester:rwa");
    meta.add_to_acl(submitterAcl);
    AccessControl duplicateAcl = BlobStoreAclHandler.parseAccessControl("u:tester:r--");
    meta.add_to_acl(duplicateAcl);
    String testKey = "testDuplicateACLsBlobKey";
    client.createBlob(testKey, meta);
  }

  @Test
  public void testGoodACLsForCreate() throws Exception {
    SettableBlobMeta meta = new SettableBlobMeta();
    AccessControl submitterAcl = BlobStoreAclHandler.parseAccessControl("u:tester:rwa");
    meta.add_to_acl(submitterAcl);
    String testKey = "testBlobKey";
    client.createBlob(testKey, meta);
    validatedBlobAcls(testKey);
  }

  @Test(expected=AuthorizationException.class)
  public void testDuplicateACLsForSetBlobMeta() throws Exception {
    String testKey = "testDuplicateACLsBlobKey";
    SettableBlobMeta meta = new SettableBlobMeta();
    createTestBlob(testKey, meta);
    AccessControl duplicateAcl = BlobStoreAclHandler.parseAccessControl("u:tester:r--");
    meta.add_to_acl(duplicateAcl);
    client.setBlobMeta(testKey, meta);
  }

  @Test
  public void testGoodACLsForSetBlobMeta() throws Exception {
    String testKey = "testBlobKey";
    SettableBlobMeta meta = new SettableBlobMeta();
    createTestBlob(testKey, meta);
    meta.add_to_acl(BlobStoreAclHandler.parseAccessControl("u:nextuser:r--"));
    client.setBlobMeta(testKey,meta);
    validatedBlobAcls(testKey);
  }

  private void createTestBlob(String testKey, SettableBlobMeta meta) throws AuthorizationException, KeyAlreadyExistsException {
    AccessControl submitterAcl = BlobStoreAclHandler.parseAccessControl("u:tester:rwa");
    meta.add_to_acl(submitterAcl);
    client.createBlob(testKey, meta);
  }

  private void validatedBlobAcls(String testKey) throws KeyNotFoundException, AuthorizationException {
    ReadableBlobMeta blobMeta = client.getBlobMeta(testKey);
    Assert.assertNotNull("The blob" + testKey + "does not have any readable blobMeta.", blobMeta);
    SettableBlobMeta settableBlob = blobMeta.get_settable();
    Assert.assertNotNull("The blob" + testKey + "does not have any settable blobMeta.", settableBlob);
  }
}
