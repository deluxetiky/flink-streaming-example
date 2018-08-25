package com.sinanbir.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FilterStreamParser implements FlatMapFunction<String,Tuple2<String,Boolean>>{
	@Override
	public void flatMap(String s, Collector<Tuple2<String,Boolean>> collector) throws Exception{
		String[] strVals = s.split("=");
		if(strVals.length == 2){
			String key = strVals[0];
			Boolean val = strVals[1].equals("1") || strVals[1].equals("true");
			collector.collect(Tuple2.of(key, val));
		}
	}
}
