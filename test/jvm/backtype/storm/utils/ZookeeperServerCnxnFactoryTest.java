package backtype.storm.utils;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class ZookeeperServerCnxnFactoryTest {

  @Test(expected=RuntimeException.class)
  public void testConstructorThrowsRuntimeExceptionIfPortNumTooLarge() {
    int tooLargePortNum = 65536;
    int arbitraryMaxClients = 1;
    new ZookeeperServerCnxnFactory(tooLargePortNum, arbitraryMaxClients);
  }

  @Test
  public void testFactoryHasCorrectMaxNumClients() {
    int arbitraryPort = 2000;
    int arbitraryMaxClients = 42;
    ZookeeperServerCnxnFactory zkcf =
      new ZookeeperServerCnxnFactory(arbitraryPort, arbitraryMaxClients);

    assertThat(zkcf.factory().getMaxClientCnxnsPerHost(), is(arbitraryMaxClients));
  }

  @Test
  public void testConstructorHandlesNegativePort() {
    int negativePort = -42;
    int arbitraryMaxClients = 42;
    assertThat(new ZookeeperServerCnxnFactory(negativePort, arbitraryMaxClients), notNullValue());
  }
}
