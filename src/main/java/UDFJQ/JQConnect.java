package UDFJQ;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @Description: 测试自定义sink的鉴权功能
 * @ClassName: JQConnect
 * @Author: lemon
 * @Date: 2020/7/20 15:20
 * @Version: 1.0
 */
public class JQConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        String SourcebootstrapServers = "172.18.194.xxx:9092,172.18.194.xxx:9092,172.18.194.xxx:9092";
        String SinkbootstrapServers = "172.18.194.xxx:9092,172.18.194.xxx:9092,172.18.194.xxx:9092";
        String Topic = "flink2KafkaTest";
        String groupId = "group1";
        String Kafkausername = "admin";
        String Kafkapassword = "adminpassword";
        String Redispassword = "redispassword";


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", SourcebootstrapServers);	// kafka集群地址
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<String>(Topic, new SimpleStringSchema(), properties);
        DataStream<String> dataStream =
                environment.addSource(consumer);
        dataStream.print();

        //Kafka 自定义Sink + 鉴权
        dataStream.addSink(new JQKafkaSink(SinkbootstrapServers,Topic,Kafkausername,Kafkapassword));

        //Redis 自定义Sink + 鉴权
        Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort("132.98.26.xxx", 7001));
        nodes.add(new HostAndPort("132.98.26.xxx", 7002));
        nodes.add(new HostAndPort("132.98.26.xxx", 7001));
        nodes.add(new HostAndPort("132.98.26.xxx", 7002));
        JQStringRedisSink redisSink = new JQStringRedisSink(nodes, Redispassword);
        //这里传入的类型是 String,String 所以 要做一个 数据类型转换，可以根据业务对数据切分 赋值
        SingleOutputStreamOperator<Tuple2<String, String>> flatMap = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                Tuple2<String, String> tuple2 = new Tuple2<>();
                int index = value.indexOf(":");
                tuple2.f0 = value.substring(0,index);
                tuple2.f1 = value.substring(index+1);
                out.collect(tuple2);
            }
        });
        flatMap.addSink(redisSink);

        environment.execute();
    }
}
