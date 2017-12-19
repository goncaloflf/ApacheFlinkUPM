package es.upm.master;

/**
 * Created by Goncalo on 18/12/2017.
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class firsttry {

    public static void main(String[] args)  throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String	inFilePath = "C:\\Users\\Goncalo\\IdeaProjects\\flinkPr\\projectFlink\\traffic-3xways";
        String	outFilePath = "C:\\Users\\Goncalo\\IdeaProjects\\flinkPr\\projectFlink\\luisa";
        System.out.println("Started to read the file...");
        DataStreamSource<String> source	= env.readTextFile(inFilePath).setParallelism(1);

        DataStream<Integer> mapped = source.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) {
                String s = value.split(",")[0];
                System.out.println(s);
                return Integer.parseInt(s);
            }
        });

        source.writeAsText(outFilePath);
        env.execute();
    }
}
