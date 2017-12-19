package es.upm.master;

/**
 * Created by Goncalo on 18/12/2017.
 */

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class firsttry {

    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String	inFilePath = "C:\\Users\\Goncalo\\IdeaProjects\\flinkPr\\projectFlink\\traffic-3xways";
        String	outFilePath = "C:\\Users\\Goncalo\\IdeaProjects\\flinkPr\\projectFlink\\test.txt";
        System.out.println("Started to read the file...");
        DataStreamSource<String> source	= env.readTextFile(inFilePath);

    }
}
