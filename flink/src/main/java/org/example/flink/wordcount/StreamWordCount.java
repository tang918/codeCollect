package org.example.flink.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流式处理
 * */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //1. 创建流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 读取文本流
        DataStreamSource<String> lineDataStream = env.socketTextStream("127.0.0.1", 7777);

        //3. 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOne.keyBy(data -> data.f0);
        //5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

        sum.print();
        //7.启动执行
        env.execute();


    }
}
