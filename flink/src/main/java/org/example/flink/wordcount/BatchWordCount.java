package org.example.flink.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 *flink实现 批处理wordCount
 * */
public class BatchWordCount {
    private static final String FILE_PATH="input/words.txt";
    public static void main(String[] args)throws Exception {
        //1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2 从文件中读取数据
        DataSource<String> lineDataSource = env.readTextFile(FILE_PATH);

        //3. 将每行数据进行分词，转换成二元组类型
        FlatMapOperator<String, Tuple2<String,Long>> wordAndOne = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4. 按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> tuple2UnsortedGrouping = wordAndOne.groupBy(0);
        // 5. 分组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = tuple2UnsortedGrouping.sum(1);
        //6. 打印结果
        sum.print();



    }

    /**
     * 测试文件路径
     * */
    private static  void testReadFile() throws IOException {
        String path="input/words.txt";
        File file = new File(path);
        System.out.println(file.getAbsoluteFile());
        System.out.println(file.getPath());
        FileInputStream fin=null;
        try{
            fin=new FileInputStream(path);
            byte b[] = new byte[fin.available()];
            fin.read(b);
            String str = new String(b);
            System.out.println(str);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(fin!=null){
                fin.close();
            }
        }


    }
}
