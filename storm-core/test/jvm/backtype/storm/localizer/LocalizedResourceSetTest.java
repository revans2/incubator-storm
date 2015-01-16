package backtype.storm.localizer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LocalizedResourceSetTest {

  private final String user1 = "user1";

  @Test
  public void testGetUser() throws Exception {
    LocalizedResourceSet lrset = new LocalizedResourceSet(user1);
    assertEquals("user is wrong", user1, lrset.getUser());
  }

  @Test
  public void testGetSize() throws Exception {
    LocalizedResourceSet lrset = new LocalizedResourceSet(user1);
    LocalizedResource localresource1 = new LocalizedResource("key1", "testfile1", false, "topo1");
    LocalizedResource localresource2 = new LocalizedResource("key2", "testfile2", true, "topo1");
    assertEquals("size is wrong", 0, lrset.getSize());
    lrset.addResource("key1", localresource1, false);
    assertEquals("size is wrong", 1, lrset.getSize());
    lrset.addResource("key2", localresource2, true);
    assertEquals("size is wrong", 2, lrset.getSize());
  }

  @Test
  public void testGet() throws Exception {
    LocalizedResourceSet lrset = new LocalizedResourceSet(user1);
    LocalizedResource localresource1 = new LocalizedResource("key1", "testfile1", false, "topo1");
    LocalizedResource localresource2 = new LocalizedResource("key2", "testfile2", true, "topo1");
    lrset.addResource("key1", localresource1, false);
    lrset.addResource("key2", localresource2, true);
    assertEquals("get doesn't return same object", localresource1, lrset.get("key1", false));
    assertEquals("get doesn't return same object", localresource2, lrset.get("key2", true));

  }

  @Test
  public void testExists() throws Exception {
    LocalizedResourceSet lrset = new LocalizedResourceSet(user1);
    LocalizedResource localresource1 = new LocalizedResource("key1", "testfile1", false, "topo1");
    LocalizedResource localresource2 = new LocalizedResource("key2", "testfile2", true, "topo1");
    lrset.addResource("key1", localresource1, false);
    lrset.addResource("key2", localresource2, true);
    assertEquals("doesn't exist", true, lrset.exists("key1", false));
    assertEquals("doesn't exist", true, lrset.exists("key2", true));
    boolean val = lrset.remove(localresource1);
    assertTrue("remove failed", val);
    assertEquals("does exist", false, lrset.exists("key1", false));
    assertEquals("doesn't exist", true, lrset.exists("key2", true));
    val = lrset.remove(localresource1);
    assertFalse("remove success when shouldn't have been", val);
  }
}
