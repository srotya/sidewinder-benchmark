/**
 * Copyright 2018 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.clients;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import com.srotya.sidewinder.core.rpc.BatchData;
import com.srotya.sidewinder.core.rpc.BatchData.Builder;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc.WriterServiceBlockingStub;

import io.grpc.CompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class StockLoader2 {

	public static void main(String[] args) throws IOException, KeyManagementException, NoSuchAlgorithmException,
			KeyStoreException, ParseException, InterruptedException, ExecutionException {

		ExecutorService es = Executors.newFixedThreadPool(4);

		File[] listFiles = new File(args[0]).listFiles();
		for (int i = 0; i < listFiles.length; i++) {
			final int p = i;
			es.submit(() -> {
				try {
					int counter = 0;
					final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9928)
							.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();
					File file = listFiles[p];
					String ticker = file.getName().replace(".csv", "").split("_")[1];
					BufferedReader reader = new BufferedReader(new FileReader(file));
					String temp = null;
					SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
					Builder builder = BatchData.newBuilder();

					while ((temp = reader.readLine()) != null) {
						String[] splits = temp.split(",");
						Date timestamp = format.parse(splits[0]);
						long ts = timestamp.getTime();

						com.srotya.sidewinder.core.rpc.Point.Builder pointBuilder = Point.newBuilder()
								.setDbName("stocks").setMeasurementName("ticker").setTimestamp(ts)
								.addTags(Tag.newBuilder().setTagKey("symbol").setTagValue(ticker));

						pointBuilder.addValueFieldName("open").addFp(true)
								.addValue(Double.doubleToLongBits(Double.parseDouble(splits[2])));

						pointBuilder.addValueFieldName("high").addFp(true)
								.addValue(Double.doubleToLongBits(Double.parseDouble(splits[3])));

						pointBuilder.addValueFieldName("low").addFp(true)
								.addValue(Double.doubleToLongBits(Double.parseDouble(splits[4])));

						pointBuilder.addValueFieldName("close").addFp(true)
								.addValue(Double.doubleToLongBits(Double.parseDouble(splits[5])));

						pointBuilder.addValueFieldName("volume").addFp(true)
								.addValue(Double.doubleToLongBits(Double.parseDouble(splits[6])));
						
						counter++;
						if (counter % 1000 == 0) {
							WriterServiceBlockingStub writer = WriterServiceGrpc.newBlockingStub(channel);
							writer.writeBatchDataPoint(builder.setMessageId(System.nanoTime()).build());
							builder = BatchData.newBuilder();
						}
					}
					WriterServiceBlockingStub writer = WriterServiceGrpc.newBlockingStub(channel);
					writer.writeBatchDataPoint(builder.setMessageId(System.nanoTime()).build());
					reader.close();
					channel.shutdownNow();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			// Thread.sleep(10);
		}
		es.shutdown();
		es.awaitTermination(1000, TimeUnit.SECONDS);
	}

	public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout)
			throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		HttpClientBuilder clientBuilder = HttpClients.custom();
		RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(requestTimeout).build();

		return clientBuilder.setDefaultRequestConfig(config).build();
	}
}
