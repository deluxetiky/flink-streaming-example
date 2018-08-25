package com.sinanbir.stream;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class JoinProcessFunction extends CoProcessFunction<Tuple2<String,Integer>,Tuple2<String,Boolean>,Tuple2<String,Integer>>{
	private ValueState<FilterState> state;

	@Override
	public void open(Configuration parameters) throws Exception{
		ValueStateDescriptor<FilterState> desc = new ValueStateDescriptor<>(
				"filterState",
				FilterState.class, new FilterState(true)
		);
		state = getRuntimeContext().getState(desc);
	}

	@Override
	public void processElement1(Tuple2<String,Integer> input, Context context, Collector<Tuple2<String,Integer>> collector) throws Exception{
		String key = input.f0;
		FilterState current = state.value();
		if(current.Active) collector.collect(input);//filtering place
	}

	@Override
	public void processElement2(Tuple2<String,Boolean> input, Context context, Collector<Tuple2<String,Integer>> collector) throws Exception{
		FilterState current = state.value();
		current.Active = input.f1;//set state value
		state.update(current);
	}
}
