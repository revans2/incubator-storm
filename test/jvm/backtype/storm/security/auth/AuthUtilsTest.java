package backtype.storm.security.auth;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthUtilsTest {

  Configuration conf;
  List<AppConfigurationEntry> entries;
  AppConfigurationEntry emptyEntry;

  @SuppressWarnings("unchecked")
  private AppConfigurationEntry generateMockAppConfigEntry() {
    AppConfigurationEntry toRet = mock(AppConfigurationEntry.class);
    when((Map<String,Object>)toRet.getOptions())
        .thenReturn(new HashMap<String,Object>());
    return toRet;
  }

  @Before
  public void setup() {
    conf = mock(Configuration.class);
    emptyEntry = generateMockAppConfigEntry();
  }

  @Test(expected=java.io.IOException.class)
  public void testThrowsOnMissingSeciton() throws IOException {
    Configuration conf = mock(Configuration.class);
    AuthUtils.get(conf, "bogus-section", "");
  }

  @Test
  public void testReturnsNullIfNoSuchSection() throws IOException {
    AppConfigurationEntry[] entries = { emptyEntry };
    final String sectionName = "bogus-section";
    when(conf.getAppConfigurationEntry(sectionName)).thenReturn(entries);
    assertNull(AuthUtils.get(conf, sectionName, "nonexistent-key"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReturnsFirstValueForValidKey() throws IOException {
    final String key = "the-key";

    AppConfigurationEntry entryWithBadValue = generateMockAppConfigEntry();
    ((Map<String,Object>)entryWithBadValue.getOptions()).put("the-key", (Object) "bad-value");

    AppConfigurationEntry entryWithGoodValue = generateMockAppConfigEntry();
    final String expected = "good-value";
    ((Map<String,Object>)entryWithGoodValue.getOptions()).put("the-key", (Object) expected);

    AppConfigurationEntry[] entries = 
        { emptyEntry, entryWithGoodValue, entryWithBadValue };
    final String sectionName = "bogus-section";
    when(conf.getAppConfigurationEntry(sectionName)).thenReturn(entries);

    final String actual = AuthUtils.get(conf, sectionName, key);
    assertNotNull(actual);
    assertEquals(expected, actual);
  }
}
