package de.bussche.flink;

/**
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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final TwitterProperties properties = new TwitterProperties();

		DataStream<String> streamSource = env.addSource(new TwitterSource(properties.getProperties()));

		DataStream<String> textStream = streamSource.map(new MapFunction<String, String>() {
			private transient ObjectMapper jsonParser;

			@Override
			public String map(String tweet) throws Exception {
				String text = "n/a";
				if (jsonParser == null) {
					jsonParser = new ObjectMapper();
				}
				try {
					JsonNode jsonNode = jsonParser.readValue(tweet, JsonNode.class);
					boolean hasText = jsonNode.has("text");
					if (hasText) {
						text = jsonNode.get("text").asText();
					}
				} catch (Exception e) {

				}
				return text;
			}
		});

		textStream.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
