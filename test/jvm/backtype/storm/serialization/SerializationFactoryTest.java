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
  public void testRegistersDefaultSerializerWhenNoConf() {
    this.conf.remove(Config.TOPOLOGY_TUPLE_SERIALIZER);
    Kryo k = SerializationFactory.getKryo(this.conf);

    // Disable overriding the default class, so that we can get the default was
    // used before it was overridden.
    ((DefaultKryoFactory.KryoSerializableDefault)k).overrideDefault(false);
    Class originalDefault = k.getDefaultSerializer(ListDelegate.class).getClass();
    assertThat(k.getSerializer(ListDelegate.class), instanceOf(originalDefault));

    // Test the same with override on, just in case.
    ((DefaultKryoFactory.KryoSerializableDefault)k).overrideDefault(true);
    assertThat(k.getSerializer(ListDelegate.class), instanceOf(originalDefault));
  }

  @Test
  public void testRegistersDefaultSerializerWhenNoSuchClass() {
    conf.put(Config.TOPOLOGY_TUPLE_SERIALIZER, "null.this.class.does.not.exist");
    Kryo k = SerializationFactory.getKryo(this.conf);

    // Disable overriding the default class, so that we can get the default was
    // used before it was overridden.
    ((DefaultKryoFactory.KryoSerializableDefault)k).overrideDefault(false);
    Class originalDefault = k.getDefaultSerializer(ListDelegate.class).getClass();
    assertThat(k.getSerializer(ListDelegate.class), instanceOf(originalDefault));

    // Test the same with override on, just in case.
    ((DefaultKryoFactory.KryoSerializableDefault)k).overrideDefault(true);
    assertThat(k.getSerializer(ListDelegate.class), instanceOf(originalDefault));
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
