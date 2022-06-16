package org.example.flink.dataStreamAPI;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.common.Event;

/**
 * 用户自定义算子(富函数类)
 * */
public class RichFunctionTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1，即串行
        env.setParallelism(1);
        // 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "/home", 100L),
                new Event("Bob", "./card", 2000L),
                new Event("Mary", "/test", 100L),
                new Event("Bob", "./home", 2000L),
                new Event("Alisa", "/home", 4000L),
                new Event("Bob", "./home", 8000L)


        );
        stream.map(new MyRicherMapper()).print();
        env.execute();
    }


    //实现一个自定义的富函数类
    public static class MyRicherMapper extends RichMapFunction<Event,Integer>{

        //在调用map函数之前会自动执行Open函数
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用....."+getRuntimeContext().getTaskName());
        }

        //自定义的map算子需要重写map方法
        @Override
        public Integer map(Event event) throws Exception {
            return event.url.length();
        }

        //在调用map函数之后会自动执行close()函数
        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期被调用");
        }
    }

}


