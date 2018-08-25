/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sinanbir.example;

import com.sinanbir.stream.FilterStreamParser;
import com.sinanbir.stream.JoinProcessFunction;
import com.sinanbir.stream.WordStreamParser;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob{

	public static void main(String[] args) throws Exception{

		final ParameterTool params = ParameterTool.fromArgs(args);
		int portWordStream = params.getInt("portStream");
		int portFilterStream = params.getInt("portFilterStream");
		int portSink = params.getInt("portSink");
		String stateDir = params.get("stateDir");
		String socketHost = params.get("host");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setStateBackend(new RocksDBStateBackend(stateDir, false));
		env.enableCheckpointing(2000);// start a checkpoint every 2seconds
		CheckpointConfig config = env.getCheckpointConfig();
		config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// set mode to exactly-once (this is the default)
		config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		DataStream<Tuple2<String,Integer>> wordStream = env
				.socketTextStream(socketHost, portWordStream).name("Word Stream")
				.setParallelism(1)
				.flatMap(new WordStreamParser()).name("Word FlatMap")
				.keyBy(0);

		DataStream<Tuple2<String,Boolean>> filterStream = env
				.socketTextStream(socketHost, portFilterStream).name("Filter Stream")
				.setParallelism(1)
				.flatMap(new FilterStreamParser()).name("Filter FlatMap")
				.keyBy(0);

		DataStream<Tuple2<String,Integer>> joinedStream = wordStream
				.connect(filterStream)
				.process(new JoinProcessFunction()).setParallelism(5).uid("join-1").name("Co-JoinProcess")
				.keyBy(0)
				.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
				.sum(1).name("Summarize")
				.setParallelism(5);

		joinedStream.map((MapFunction<Tuple2<String,Integer>,String>)input -> String.format("%tT | %s Count: %d\n", LocalDateTime.now(), input.f0, input.f1))
				.returns(String.class)
				.writeToSocket(socketHost, portSink, new SimpleStringSchema())
				.setParallelism(1)
				.name("Socket Output");


		// execute program
		env.execute("Flink Streaming Stateful Java Example");
	}
}
