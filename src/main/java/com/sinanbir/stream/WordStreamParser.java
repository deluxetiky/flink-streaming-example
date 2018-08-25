package com.sinanbir.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordStreamParser implements FlatMapFunction<String,Tuple2<String,Integer>>{
	@Override
	public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception{
		for(String word : s.trim().split(" ")) {
			collector.collect(Tuple2.of(word, 1));
		}
	}
}
