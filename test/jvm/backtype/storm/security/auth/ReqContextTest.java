package backtype.storm.security.auth;
import org.junit.Test;

import java.net.UnknownHostException;
import java.security.AccessControlContext;
import java.security.Principal;
import java.security.ProtectionDomain;

import java.net.InetAddress;
import java.security.AccessController;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.HashSet;

import javax.security.auth.Subject;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.mockito.Mockito.mock;

public class ReqContextTest {

  @Test
  public void testSubject() {
    ReqContext rc = ReqContext.context();
    Subject expected = new Subject();
    assertFalse(expected.isReadOnly());
    rc.setSubject(expected);
    assertEquals(expected, rc.subject());

    // Change the Subject by setting read-only.
    expected.setReadOnly();
    rc.setSubject(expected);
    assertEquals(expected, rc.subject());
  }

  @Test
  public void testRemoteAddress() throws UnknownHostException {
    ReqContext rc = ReqContext.context();
    InetAddress expected = InetAddress.getByAddress("ABCD".getBytes());
    rc.setRemoteAddress(expected);
    assertEquals(expected, rc.remoteAddress());
  }


  @Test
  public void testPrincipalReturnsNullWhenNoSubjectOrPrincipals() {
    ProtectionDomain[] domains = {};
    AccessControlContext acc = new AccessControlContext(domains);
    ReqContext rc = new ReqContext(acc);
    assertNull(rc.principal());

    Subject s = new Subject();
    rc.setSubject(s);
    assertNull(rc.principal());
  }

  private static final AtomicInteger uniquePrincipalId = new AtomicInteger(0);
  private final String testPrincipalName = "Test Principal";

  private class TestPrincipal implements Principal {
    private final int hashCode;

    public TestPrincipal() {
      this.hashCode = uniquePrincipalId.incrementAndGet();
    }

    @Override
    public boolean equals(Object other) {
      return this.hashCode == other.hashCode();
    }

    @Override
    public String getName() {
      return testPrincipalName;
    }

    @Override
    public int hashCode() {
      return this.hashCode;
    }
  }

  @Test
  public void testPrincipalReturnsCorrectPrincipal() {
    Principal p = new TestPrincipal();
    Set<Principal> principals = new HashSet<Principal>();
    principals.add(p);
    Set<Object> creds = new HashSet<Object>();
    Subject s = new Subject(false, principals, creds, creds);

    ReqContext rc = ReqContext.context();
    rc.setSubject(s);

    assertNotNull(rc.principal());
    assertEquals(testPrincipalName, rc.principal().getName());
  }

  @Test
  public void testOperation() {
    ReqContext rc = ReqContext.context();
    ReqContext.OperationType expected = ReqContext.OperationType.SUBMIT_TOPOLOGY;
    assertThat(rc.operation(), is(not(expected)));

    rc.setOperation(expected);
    assertEquals(expected, rc.operation());
  }
}
