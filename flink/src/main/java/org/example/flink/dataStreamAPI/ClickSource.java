package org.example.flink.dataStreamAPI;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.flink.common.Event;

public class ClickSource implements SourceFunction<Event> {

    //声明一个标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {


        //循环生成数据
        while (running){
            sourceContext.collect(new Event());
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
