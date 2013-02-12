package backtype.storm.serialization;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.util.Map;

import com.esotericsoftware.kryo.Kryo;

import backtype.storm.Config;
import backtype.storm.security.serialization.BlowfishTupleSerializer;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.serialization.types.ListDelegateSerializer;
import backtype.storm.utils.ListDelegate;
import backtype.storm.utils.Utils;


public class SerializationFactoryTest {

  private Map<String,Object> conf;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    conf = (Map<String,Object>)Utils.readDefaultConfig();
  }

  @Test
  public void testRegistersDefaultSerializerWhenNotSpecifiedinStormConf()
      throws ClassNotFoundException {
    String configuredClassName
        = (String) this.conf.get(Config.TOPOLOGY_TUPLE_SERIALIZER);
    Class configuredClass = Class.forName(configuredClassName);

    // Do not read the stormConfig, just go with defaults.
    Kryo k = SerializationFactory.getKryo(this.conf);
    assertThat(k.getSerializer(ListDelegate.class), is(configuredClass));
  }

  @Test(expected=RuntimeException.class)
  public void testThrowsRuntimeExceptionWhenNoSuchClass() {
    conf.put(Config.TOPOLOGY_TUPLE_SERIALIZER, "null.this.class.does.not.exist");
    Kryo k = SerializationFactory.getKryo(this.conf);
  }

  @Test
  public void testRegistersWhenValidClassName() throws ClassNotFoundException {
    String arbitraryClassName =
        "backtype.storm.security.serialization.BlowfishTupleSerializer";
    Class serializerClass  = Class.forName(arbitraryClassName);
    conf.put(Config.TOPOLOGY_TUPLE_SERIALIZER, arbitraryClassName);

    // Required for construction of the class.
    String arbitraryKey = "0123456789abcdef";
    conf.put(BlowfishTupleSerializer.SECRET_KEY, arbitraryKey);

    Kryo k = SerializationFactory.getKryo(this.conf);
    assertThat(k.getSerializer(ListDelegate.class), is(serializerClass));
  }
}
