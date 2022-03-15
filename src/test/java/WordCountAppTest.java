import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kafka.stream.WordCountApp;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class WordCountAppTest {

    TopologyTestDriver testDriver;

    private Serde<String> stringSerde = new Serdes.StringSerde();
    private Serde<Long> longSerde = new Serdes.LongSerde();

    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;


    @Before
    public void setUpTopologyTestDriver(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        WordCountApp wordCountApp = new WordCountApp();
        Topology topology = wordCountApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
        this.inputTopic = testDriver.createInputTopic("word-count-input", stringSerde.serializer(), stringSerde.serializer());
        this.outputTopic = testDriver.createOutputTopic("word-count-output", stringSerde.deserializer(), longSerde.deserializer());
    }

    @After
    public void closeTestDriver(){
        testDriver.close();
    }

    public void pushNewInputRecord(String value){
        inputTopic.pipeInput(value);
    }

    @Test
    public void dummyTest(){
        String dummy = "Du" + "mmy";
        assertEquals(dummy, "Dummy");
    }

    public KeyValue<String, Long> readOutput(){
        //return testDriver.readOutput("word-count-output", new StringDeserializer(), new LongDeserializer());
        return outputTopic.readKeyValue();
    }

    @Test
    public void makeSureCountsAreCorrect(){
        String firstExample = "testing Kafka Streams";
        pushNewInputRecord(firstExample);
        assertEquals(readOutput(), new KeyValue<>("testing", 1L));
        assertEquals(readOutput(), new KeyValue<>("kafka", 1L));
        assertEquals(readOutput(), new KeyValue<>("streams", 1L));
        //this is not returning null anymore, but an exception
        //assertEquals(readOutput(), null);

        String secondExample = "testing Kafka again";
        pushNewInputRecord(secondExample);
        assertEquals(readOutput(), new KeyValue<>("testing", 2L));
        assertEquals(readOutput(), new KeyValue<>("kafka", 2L));
        assertEquals(readOutput(), new KeyValue<>("again", 1L));

    }

    @Test
    public void makeSureWordsBecomeLowercase(){
        String upperCaseString = "KAFKA kafka Kafka";
        pushNewInputRecord(upperCaseString);
        assertEquals(readOutput(), new KeyValue<>("kafka", 1L));
        assertEquals(readOutput(), new KeyValue<>("kafka", 2L));
        assertEquals(readOutput(), new KeyValue<>("kafka", 3L));

    }
}
