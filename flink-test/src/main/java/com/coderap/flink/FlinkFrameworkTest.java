package com.coderap.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class FlinkFrameworkTest {

    private static final StreamExecutionEnvironment streamExecutionEnvironment;

    static {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        configuration.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
        configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, 1);

        streamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
    }

    @Test
    public void testFlinkHelloWorld() throws Exception {
        DataStreamSource<String> lines = streamExecutionEnvironment.socketTextStream("localhost", 8080);
        lines.print();

        streamExecutionEnvironment.execute();
    }

    @Test
    public void testFlinkHelloWorld2() throws Exception {
        DataStreamSource<String> lines = streamExecutionEnvironment.socketTextStream("localhost", 8080);

        SingleOutputStreamOperator<Long> map = lines.map(((line -> Long.parseLong(line))));

        map.print();

        streamExecutionEnvironment.execute();
    }

    @Test
    public void testFlinkHelloWorld3() throws Exception {
        DataStreamSource<String> lines = streamExecutionEnvironment.socketTextStream("localhost", 8080);

        SingleOutputStreamOperator<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                if (value.contains(",")) {
                    String[] values = value.split(",");
                    for (String temp : values) {
                        out.collect(temp);
                    }
                } else {
                    out.collect(value);
                }
            }
        });

        flatMap.print();

        streamExecutionEnvironment.execute();
    }

    @Test
    public void testFlinkTransformation() throws Exception {
        DataStreamSource<String> lines = streamExecutionEnvironment.socketTextStream("localhost", 8080);

        SingleOutputStreamOperator<Long> map = lines.map(((line -> Long.parseLong(line))));

        map.print();

        streamExecutionEnvironment.execute();
    }


}
