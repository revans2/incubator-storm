package backtype.storm.blobstore;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HdfsBlobStoreImplTest {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsBlobStoreImplTest.class);

  protected static Configuration hadoopConf;
  protected static MiniDFSCluster dfscluster;
  // key dir needs to be number 0 to number of buckets, choose one so we know where to look
  private static String KEYDIR = "0";
  private Path blobDir = new Path("/storm/blobstore1");
  private Path fullKeyDir = new Path(blobDir, KEYDIR);

  public class TestHdfsBlobStoreImpl extends HdfsBlobStoreImpl {

    public TestHdfsBlobStoreImpl(Path path, Map<String, Object> conf) throws IOException {
      super(path, conf);
    }

    public TestHdfsBlobStoreImpl(Path path, Map<String, Object> conf,
                                 Configuration hconf) throws IOException {
      super(path, conf, hconf);
    }

    protected Path getKeyDir(String key) {
      return new Path(new Path(blobDir, KEYDIR), key);
    }
  }

  @BeforeClass
  public static void init() {
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
  }

  @AfterClass
  public static void cleanup() throws IOException {
    if (dfscluster != null) {
      dfscluster.shutdown();
    }
  }

  // Be careful about adding additional tests as the dfscluster will be shared

  @Test
  public void testMultiple() throws Exception {
    String testString = "testingblob";
    String validKey = "validkeyBasic";

    FileSystem fs = dfscluster.getFileSystem();
    Map conf = new HashMap();

    TestHdfsBlobStoreImpl hbs = new TestHdfsBlobStoreImpl(blobDir, conf, hadoopConf);
    // should have created blobDir
    assertTrue("BlobStore dir wasn't created", fs.exists(blobDir));
    assertEquals("BlobStore dir was created with wrong permissions",
        HdfsBlobStoreImpl.BLOBSTORE_DIR_PERMISSION, fs.getFileStatus(blobDir).getPermission());

    // test exist with non-existent key
    assertFalse("file exists but shouldn't", hbs.exists("bogus"));

    // test write
    BlobStoreFile pfile = hbs.write(validKey, false);
    OutputStream ios = pfile.getOutputStream();
    ios.write(testString.getBytes(Charset.forName("UTF-8")));
    ios.close();

    // test commit creates properly
    assertTrue("BlobStore key dir wasn't created", fs.exists(fullKeyDir));
    pfile.commit();
    Path dataFile = new Path(new Path(fullKeyDir, validKey), BlobStoreFile.BLOBSTORE_DATA_FILE);
    assertTrue("blob data not committed", fs.exists(dataFile));
    assertEquals("BlobStore dir was created with wrong permissions",
        HdfsBlobStoreFile.BLOBSTORE_FILE_PERMISSION, fs.getFileStatus(dataFile).getPermission());
    assertTrue("key doesn't exist but should", hbs.exists(validKey));

    // test read
    BlobStoreFile readpFile = hbs.read(validKey);
    String readString = IOUtils.toString(readpFile.getInputStream(), "UTF-8");
    assertEquals("string read from blob doesn't match", testString, readString);

    // test listkeys
    Iterator<String> keys = hbs.listKeys();
    assertTrue("blob has one key", keys.hasNext());
    assertEquals("one key in blobstore", validKey, keys.next());

    // delete
    hbs.deleteKey(validKey);
    assertFalse("key not deleted", fs.exists(dataFile));
    assertFalse("key not deleted", hbs.exists(validKey));

    // Now do multiple
    String testString2 = "testingblob2";
    String validKey2= "validkey2";

    // test write
    pfile = hbs.write(validKey, false);
    ios = pfile.getOutputStream();
    ios.write(testString.getBytes(Charset.forName("UTF-8")));
    ios.close();

    // test commit creates properly
    assertTrue("BlobStore key dir wasn't created", fs.exists(fullKeyDir));
    pfile.commit();
    assertTrue("blob data not committed", fs.exists(dataFile));
    assertEquals("BlobStore dir was created with wrong permissions",
        HdfsBlobStoreFile.BLOBSTORE_FILE_PERMISSION, fs.getFileStatus(dataFile).getPermission());
    assertTrue("key doesn't exist but should", hbs.exists(validKey));

    // test write again
    pfile = hbs.write(validKey2, false);
    OutputStream ios2 = pfile.getOutputStream();
    ios2.write(testString2.getBytes(Charset.forName("UTF-8")));
    ios2.close();

    // test commit second creates properly
    pfile.commit();
    Path dataFile2 = new Path(new Path(fullKeyDir, validKey2), BlobStoreFile.BLOBSTORE_DATA_FILE);
    assertTrue("blob data not committed", fs.exists(dataFile2));
    assertEquals("BlobStore dir was created with wrong permissions",
        HdfsBlobStoreFile.BLOBSTORE_FILE_PERMISSION, fs.getFileStatus(dataFile2).getPermission());
    assertTrue("key doesn't exist but should", hbs.exists(validKey2));

    // test listkeys
    keys = hbs.listKeys();
    int total = 0;
    boolean key1Found = false;
    boolean key2Found = false;
    while(keys.hasNext()) {
      total++;
      String key = keys.next();
      if (key.equals(validKey)) {
        key1Found = true;
      } else if (key.equals(validKey2)) {
        key2Found = true;
      } else {
        fail("Found key that wasn't expected: " + key);
      }
    }
    assertEquals("number of keys is wrong", 2, total);
    assertTrue("blobstore missing key1", key1Found);
    assertTrue("blobstore missing key2", key2Found);

    // test read
    readpFile = hbs.read(validKey);
    readString = IOUtils.toString(readpFile.getInputStream(), "UTF-8");
    assertEquals("string read from blob doesn't match", testString, readString);

    // test read
    readpFile = hbs.read(validKey2);
    readString = IOUtils.toString(readpFile.getInputStream(), "UTF-8");
    assertEquals("string read from blob doesn't match", testString2, readString);

    hbs.deleteKey(validKey);
    assertFalse("key not deleted", hbs.exists(validKey));
    hbs.deleteKey(validKey2);
    assertFalse("key not deleted", hbs.exists(validKey2));
  }
}