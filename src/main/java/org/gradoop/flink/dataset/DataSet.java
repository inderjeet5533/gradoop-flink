package org.gradoop.flink.dataset;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.MonthDay;

public class DataSet<T> {

    private final StreamExecutionEnvironment env;
    private DataStream<T> dataStream;

    public DataSet(StreamExecutionEnvironment env, DataStream<T> dataStream) {
        this.env = env;
        this.dataStream = dataStream;
    }

    public StreamExecutionEnvironment getExecutionEnvironment() {
        return env;
    }

    private void setDataStream(DataStream<T> dataStream){
        this.dataStream = dataStream;
    }

    public <R> DataSet<R> map(MapFunction<T, R> mapper) {
        SingleOutputStreamOperator<R> mappedStream = dataStream.map(mapper);
        return new DataSet<>(env, mappedStream);
    }

    public DataSet<T>  groupBy(int i) {
        KeyedStream<T, Tuple> keyedStream = dataStream.keyBy(i);
        this.setDataStream(keyedStream);
        return this;
    }

    public DataSet<T> sum(int i) {
        KeyedStream<T, Tuple> keyedStream = (KeyedStream<T, Tuple>) dataStream;
        this.setDataStream(keyedStream.sum(i));
        return this;
    }

    public DataSet<T> union(DataStreamSource<T> fromElements) {
        DataStream<T> unionStream = dataStream.union(fromElements);
        this.setDataStream(unionStream);
        return this;
    }

}
