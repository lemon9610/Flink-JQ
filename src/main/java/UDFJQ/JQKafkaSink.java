package UDFJQ;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class JQKafkaSink extends RichSinkFunction<String> {

    private String bootstrapServers;
    private String topic;
    private String username;
    private String password;
    private int flag;

    private ProducerRecord<String, String> record;
    private KafkaProducer<String, String> producer;

    public JQKafkaSink(String bootstrapServers,String topic,String username,String password) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.username = username;
        this.password = password;
    }

    /**
     * open方法在sink第一次启动时调用，一般用于sink的初始化操作
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        flag = 1;
        //配置Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""+username+"\" password=\""+password+"\";");

        //创建consumer
        producer = new KafkaProducer<String, String>(props);
        super.open(parameters);

    }

    /**
     * invoke方法是sink数据处理逻辑的方法，source端传来的数据都在invoke方法中进行处理
     * 其中invoke方法中第一个参数类型与RichSinkFunction<String>中的泛型对应。第二个参数
     * 为一些上下文信息
     */
    @Override
    public void invoke(String value, Context context) throws Exception {
        record = new ProducerRecord<>(topic, value);
        producer.send(record);
        //因为网络问题，要多次连接才能建立联系，这个过程中会丢失7-8条数据
        if (flag < 10){
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            flag ++;
            System.out.println("第"+flag+"次尝试");
        }
        System.out.println("Kafka:"+value);
    }

    /**
     * close方法在sink结束时调用，一般用于资源的回收操作
     */
    @Override
    public void close() throws Exception {
        producer.close();
        super.close();
    }


}
