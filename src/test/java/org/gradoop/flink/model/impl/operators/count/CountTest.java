package org.gradoop.flink.model.impl.operators.count;

import junit.framework.TestCase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.gradoop.flink.dataset.DataSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CountTest extends TestCase {

//    @Test
//    public void testCount() throws Exception {
//        // Create a test dataset
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataSet<String> testDataSet = env.fromElements("A", "B", "C", "D", "E");
//
//        // Call the count function
//        DataSet<Long> result = Count.count(testDataSet);
//
//        // Extract the result
//        Long count = result.collect().get(0);
//
//        // Assert the result
//        assertEquals(5L, count.longValue()); // Change the expected count accordingly
//    }

    @Test
    public void testCountDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> stream = env.fromElements(
                new Tuple2<>("A", 1),
                new Tuple2<>("B", 2),
                new Tuple2<>("C", 3)
        );
        List<Tuple2<String, Integer>> collectedData = new ArrayList<>();
        stream.addSink(new CollectSinkFunction<>(collectedData));

        env.execute("Collect DataStream Example");
        // Print the collected data
        for (Tuple2<String, Integer> tuple : collectedData) {
            System.out.println(tuple);
        }
    }

    private static class CollectSinkFunction<T> implements SinkFunction<T> {
        private final List<T> collectedValues;
        public CollectSinkFunction(List<T> collectedValues) {
            this.collectedValues = collectedValues;
        }

        @Override
        public void invoke(T value, Context context) throws Exception {
            collectedValues.add(value);
        }
    }
}