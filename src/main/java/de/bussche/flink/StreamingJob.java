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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

public class StreamingJob {

	public static void main(String[] args) throws Exception {

		// get the execution environment ready.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// read the properties file. Make sure a file called settings.properties is in the resources directory
		// you can have a look at settings.properties.template for how the structure should look like
		final TwitterProperties properties = new TwitterProperties();

		// add the twitter source. This twitter source uses the filtered status endpoint which accepts to filter for certain user ids
		// one would have to find those user ids out in order to configure them here
		DataStream<String> streamSource = env.addSource(new TwitterSource(properties.getProperties()));

		// filter the stream for all messages from twitter which are not empty and contain text
		// map stream data to a new string which represents the text property of the json arriving from twitter
		DataStream<String> textStream = streamSource
				.filter(new FilterFunction<String>() {
					@Override
					public boolean filter(String s) {
						return !s.isEmpty() && s.contains("text");
					}
				})
				.map(new MapFunction<String, String>() {
					private transient ObjectMapper jsonParser;
					@Override
					public String map(String tweet) throws Exception {
						String text = "";
						if (jsonParser == null) {
							jsonParser = new ObjectMapper();
						}
						JsonNode jsonNode = jsonParser.readValue(tweet, JsonNode.class);
						boolean hasText = jsonNode.has("text");
						if (hasText) {
							text = jsonNode.get("text").asText();
						}
						return text;
					}
				});

		// print the data arrving from twitter out on the console
		textStream.print();

		// let's configure the ElasticSearch node where the mapped data should be stored in
		Map<String, String> esConfig = Maps.newHashMap();
		esConfig.put("bulk.flush.max.actions", "1");
		esConfig.put("cluster.name", "flink-twitter-es");
		List<TransportAddress> transports = new ArrayList<>();
		transports.add(new InetSocketTransportAddress("1.1.1.1", 9300));

		// add the ElasticSearch node as sink for Flink to store the mapped data in
		// we would be looking for an index called 'twitter-index' and the documents will be of type 'twitter-doc'
		// the document to be stored will have one property called 'text' with a value from the twitter json
		textStream.addSink(new ElasticsearchSink<>(esConfig, transports, new IndexRequestBuilder<String>() {
			@Override
			public IndexRequest createIndexRequest(String s, RuntimeContext ctx) {
				return Requests.indexRequest()
						.index("twitter-index")
						.type("twitter-doc")
						.source("text", s)
						.opType("index");
			}
		}));

		// execute program
		env.execute("Flink Streaming Java API Twitter to ES example");
	}

}
