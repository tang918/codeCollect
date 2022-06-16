package org.example.flink.dataStreamAPI;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.flink.common.Event;

import java.sql.Timestamp;

/**
 * 转换算子
 * 1. map
 * 2. filter
 * 3. flatMap
 * */
public class Transformation {
    private static  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    static {
        env.setParallelism(1);
    }
    private static final DataStreamSource<Event> STREAM = env.fromElements(
            new Event("Mary", "/home", 100L),
            new Event("Bob", "./card", 2000L),
            new Event("Mary", "/test", 400L),
            new Event("Bob", "./home", 5000L),
            new Event("Alisa", "/home", 9000L),
            new Event("Bob", "./home", 8000L)
    );


    public static void main(String[] args)throws Exception {

        //1. map算子
    //    mapTransformation();

        //2. filter算子
     //   filterTransformation();

        //3. flatMap
      //  flatMapTransformation();

        //4. KeyBy
     //   keyByMaxTransformer();

        //5. reduce
        reduceTransformer();

        env.execute();


    }

    //map算子，一一映射
    private static void mapTransformation() throws Exception {

        // 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "/home", 100L),
                new Event("Bob", "./card", 2000L)

        );
        //进行转换计算，提出user字段
        SingleOutputStreamOperator<String> map = stream.map((Event event) -> {
            return event.getUser();
        });

        map.print();
        env.execute();
    }

    // filter过滤算子
    public static void filterTransformation(){
        // 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "/home", 100L),
                new Event("Bob", "./card", 2000L),
                new Event("Mary", "/test", 100L),
                new Event("Bob", "./home", 2000L),
                new Event("Alisa", "/home", 4000L),
                new Event("Bob", "./home", 8000L)


        );
        SingleOutputStreamOperator<Event> mary = stream.filter( event -> {
            return event.user.equals("Mary");
        });
        mary.print();

    }

    //3. flatMap
    /**
     * 将数据流中的整体(一般是集合类型)拆分成一个一个的个体使用。
     * 消费一个元素，可以产生0到多个元素
     * 将数据先进行拆分，再对拆分后的数据进行转换
     * */
    private static void flatMapTransformation(){
        // 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "/home", 100L),
                new Event("Bob", "./card", 2000L),
                new Event("Mary", "/test", 100L),
                new Event("Bob", "./home", 2000L),
                new Event("Alisa", "/home", 4000L),
                new Event("Bob", "./home", 8000L)


        );
        stream.flatMap((Event event, Collector<String> collector) -> {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(new Timestamp(event.timestamp).toString());
        }).returns(new TypeHint<String>() {
        }).print();

    }

    //4.KeyBy 按键分区(逻辑分区，同一个逻辑分区的数据一定在同一个物理分区)
    //提取用户最近访问数据
    private static void keyByMaxTransformer(){
        STREAM.keyBy(event -> {return event.user;})
                .max("timestamp").print("max: ");

        STREAM.keyBy(data->data.user)
                .maxBy("timestamp").print("maxBy:");
    }

    //5. reduce
    //访问量统计
    private static void reduceTransformer(){

        //1. 统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> reduceStream = STREAM.map(data -> {
            return Tuple2.of(data.user, 1L);
        }).returns(Types.TUPLE(Types.STRING,Types.LONG)).keyBy(data -> data.f0).reduce((value1, value2) -> {
            return Tuple2.of(value1.f0, value1.f1 + value1.f1);
        }).returns(Types.TUPLE(Types.STRING,Types.LONG));

        //2. 选取当前最活跃的用户
        // 将所有数据扔到一个key中，实际开发中慎用
       reduceStream.keyBy(data->"key").reduce((value1,value2)->{
           return value1.f1>value2.f1? value1:value2;
       }).returns(Types.TUPLE(Types.STRING,Types.LONG)).print();


    }




}
