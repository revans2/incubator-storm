package backtype.storm.security.auth;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslException;
import javax.security.sasl.Sasl;
import java.util.Map;
import java.util.HashMap;

import backtype.storm.security.auth.AnonymousAuthenticationProvider;
import backtype.storm.security.auth.AnonymousAuthenticationProvider.SaslAnonymousFactory;


public class AnonymousAuthenticationProviderTest {
  @BeforeClass
  public static void classSetup() {
    java.security.Security.addProvider(new AnonymousAuthenticationProvider());
  }

  @Test
  public void testCreateSaslClientWithAnonymousMech() throws SaslException {
    String[] mechanisms = {"foo", "bar", "", "ANONYMOUS", "", "foo", "bar"};
    String arbitraryID = "test-auth-id";
    Map<String, ?> props = new HashMap<String, Object>();
    CallbackHandler cbh = mock(CallbackHandler.class);

    SaslClient c =
        Sasl.createSaslClient(mechanisms, arbitraryID, "", "", props, cbh);

    assertTrue(c instanceof AnonymousClient);
    assertEquals(arbitraryID, ((AnonymousClient)c).username);
  }

  @Test
  public void testCreateSaslClientWithoutAnonymousMech() throws SaslException {
    String[] mechanisms = {"foo", "bar", ""};
    String arbitraryID = "test-auth-id";
    Map<String, ?> props = new HashMap<String, Object>();
    CallbackHandler cbh = mock(CallbackHandler.class);

    SaslClient c =
        Sasl.createSaslClient(mechanisms, arbitraryID, "", "", props, cbh);

    assertNull(c);
  }

  @Test
  public void testCreateSaslServerWithAnonymousMech() throws SaslException {
    String mechanism = "ANONYMOUS";
    String protocol = "test-protocol";
    Map<String, ?> props = new HashMap<String, Object>();
    CallbackHandler cbh = mock(CallbackHandler.class);

    SaslServer c = Sasl.createSaslServer(mechanism, protocol, "", props, cbh);

    assertTrue(c instanceof AnonymousServer);
    assertEquals(c.getMechanismName(), "ANONYMOUS");
  }

  @Test
  public void testCreateSaslServerWithoutAnonymousMech() throws SaslException {
    String mechanism = "not Anonymous";
    String protocol = "test-protocol";
    Map<String, ?> props = new HashMap<String, Object>();
    CallbackHandler cbh = mock(CallbackHandler.class);

    SaslServer c = Sasl.createSaslServer(mechanism, protocol, "", props, cbh);

    assertNull(c);
  }
}
