package UDFJQ;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.Set;

public class JQMapRedisSink extends RichSinkFunction<Tuple2<String, Map>> {

    private Set<HostAndPort> hostAndPorts;
    private String password;
    private JedisCluster jedis;


    public JQMapRedisSink(Set<HostAndPort> hostAndPorts, String password) {
        this.hostAndPorts = hostAndPorts;
        this.password = password;
    }



    /**
     * open方法在sink第一次启动时调用，一般用于sink的初始化操作
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //构造配置
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(-1);
        config.setMinIdle(2);
        config.setMaxIdle(-1);
        config.setMaxWaitMillis(10000);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        //初始化jedis
        jedis =
            new JedisCluster(hostAndPorts,10000, 10000, 100, password, config);
        super.open(parameters);

    }

    /**
     * invoke方法是sink数据处理逻辑的方法，source端传来的数据都在invoke方法中进行处理
     * 其中invoke方法中第一个参数类型与RichSinkFunction<String>中的泛型对应。第二个参数
     * 为一些上下文信息
     */
    @Override
    public void invoke(Tuple2<String,Map> value, Context context) throws Exception {
        jedis.hmset(value.f0,value.f1);
        System.out.println(value);
    }

    /**
     * close方法在sink结束时调用，一般用于资源的回收操作
     */
    @Override
    public void close() throws Exception {
        jedis.close();
        super.close();
    }


}
