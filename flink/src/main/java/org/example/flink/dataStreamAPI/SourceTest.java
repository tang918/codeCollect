package org.example.flink.dataStreamAPI;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.flink.common.Event;

import java.util.ArrayList;
import java.util.Properties;

/**
 * 测试数据源读取，将数据封装成一个个Event对象
 * 1. 从文件、集合、元素中读取数据，读取到的是有界的数据
 * 2. 从socket数据流
 * */
public class SourceTest {
    public static void main(String[] args) throws Exception{
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1，即串行
        env.setParallelism(1);
        //1. 从文本中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        //2. 从集合中读取数据
        ArrayList<Integer> nums=new ArrayList<>();
        nums.add(1);
        nums.add(2);
        DataStreamSource<Integer> integerDataStreamSource = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary","/home",100L));
        events.add(new Event("Bob","./card",2000L));
        DataStreamSource<Event> eventDataStreamSource = env.fromCollection(events);

        //3. 从元素读取数据
        DataStreamSource<Event> eventDataStreamSource1 = env.fromElements(
                new Event("Mary", "/home", 100L),
                new Event("Bob", "./card", 2000L)

        );

        //4. 从socket文本流中读取数据
        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 7777);

        //5.从Kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:2345");
        DataStreamSource<String> clicks = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        //6. 自定义SourceFunction
        DataStreamSource<Event> eventDataStreamSource2 = env.addSource(new ClickSource());

        //7. Flink支持的数据类型
        //BasicTypeInfo类中有常见的类型定义
        //Types工具类中定义了常见的类型
        //typeutils包下有各种复杂的类型定义


        stream1.print();
        env.execute();


    }
}
