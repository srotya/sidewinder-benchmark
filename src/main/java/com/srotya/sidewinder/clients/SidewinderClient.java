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
import java.io.FileReader;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import com.srotya.sidewinder.core.rpc.Ack;
import com.srotya.sidewinder.core.rpc.BatchData;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Point.Builder;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc.WriterServiceBlockingStub;

import io.grpc.ManagedChannelBuilder;

/**
 * Simple influx client
 *
 */
public class SidewinderClient {

	public static void main(String[] args) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException,
			ClientProtocolException, IOException, InterruptedException {
		int _THREAD = 4;
		ThreadPoolExecutor es = new ThreadPoolExecutor(_THREAD, _THREAD, Integer.MAX_VALUE, TimeUnit.SECONDS,
				new ArrayBlockingQueue<>(100));

		BufferedReader reader = new BufferedReader(new FileReader(args[0]));

		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setMaxTotal(_THREAD);
		final GenericObjectPool<WriterServiceBlockingStub> pool = new GenericObjectPool<>(
				new PooledObjectFactory<WriterServiceBlockingStub>() {

					@Override
					public PooledObject<WriterServiceBlockingStub> makeObject() throws Exception {
						return new DefaultPooledObject<WriterServiceGrpc.WriterServiceBlockingStub>(
								WriterServiceGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("localhost", 9928)
										.usePlaintext(true).build()));
					}

					@Override
					public void destroyObject(PooledObject<WriterServiceBlockingStub> p) throws Exception {
					}

					@Override
					public boolean validateObject(PooledObject<WriterServiceBlockingStub> p) {
						return true;
					}

					@Override
					public void activateObject(PooledObject<WriterServiceBlockingStub> p) throws Exception {
					}

					@Override
					public void passivateObject(PooledObject<WriterServiceBlockingStub> p) throws Exception {
					}
				}, config);
		String temp;
		int i = 0;
		StringBuilder builder = new StringBuilder();
		while ((temp = reader.readLine()) != null) {
			builder.append(temp + "\n");
			if (i % 1000 == 0) {
				final String val = builder.toString();
				builder = new StringBuilder();
				boolean submit = false;
				while (!submit) {
					try {
						es.submit(() -> {
							WriterServiceBlockingStub take = null;
							try {
								take = pool.borrowObject();
							} catch (Exception e) {
								e.printStackTrace();
								return;
							}
							List<Point> pointsFromString = pointsFromString("influx", val);
							BatchData build = BatchData.newBuilder().setMessageId(0L).addAllPoints(pointsFromString)
									.build();
							try {
								Ack acl = take.writeBatchDataPoint(build);
								if (acl.getResponseCode() != 200) {
									System.out.println("Error:" + acl.getResponseCode());
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
							pool.returnObject(take);

						});
						submit = true;
					} catch (Exception e) {
					}
				}
			}
			if (i % 100000 == 0) {
				System.out.println(i + " " + new Date());
			}
			i++;
		}
		reader.close();
		es.shutdown();
		es.awaitTermination(1000, TimeUnit.SECONDS);
		System.out.println(new Date() + "   " + i);
	}

	public static List<Point> pointsFromString(String dbName, String payload) {
		List<Point> dps = new ArrayList<>();
		String[] splits = payload.split("[\\r\\n]+");
		for (String split : splits) {
			try {
				String[] parts = split.split("\\s+");
				if (parts.length < 2 || parts.length > 3) {
					// invalid datapoint => drop
					System.out.println("Invalid dp:" + split);
					continue;
				}
				long timestamp = System.currentTimeMillis();
				if (parts.length == 3) {
					timestamp = Long.parseLong(parts[2]);
					if (parts[2].length() > 13) {
						timestamp = timestamp / (1000 * 1000);
					}
				} else {
					System.out.println("DB timestamp");
				}
				String[] key = parts[0].split(",");
				String measurementName = key[0];
				Set<String> tTags = new HashSet<>();
				for (int i = 1; i < key.length; i++) {
					tTags.add(key[i]);
				}
				List<String> tags = new ArrayList<>(tTags);
				String[] fields = parts[1].split(",");
				for (String field : fields) {
					String[] fv = field.split("=");
					String valueFieldName = fv[0];
					if (!fv[1].endsWith("i")) {
						double value = Double.parseDouble(fv[1]);
						Builder builder = Point.newBuilder();
						builder.setDbName(dbName);
						builder.setMeasurementName(measurementName);
						builder.setValueFieldName(valueFieldName);
						builder.setValue(Double.doubleToLongBits(value));
						builder.addAllTags(tags);
						builder.setTimestamp(timestamp);
						builder.setFp(true);
						dps.add(builder.build());
					} else {
						fv[1] = fv[1].substring(0, fv[1].length() - 1);
						long value = Long.parseLong(fv[1]);
						Builder builder = Point.newBuilder();
						builder.setDbName(dbName);
						builder.setMeasurementName(measurementName);
						builder.setValueFieldName(valueFieldName);
						builder.setValue(value);
						builder.addAllTags(tags);
						builder.setTimestamp(timestamp);
						builder.setFp(false);
						dps.add(builder.build());
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return dps;
	}

	public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout)
			throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		HttpClientBuilder clientBuilder = HttpClients.custom();
		RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(requestTimeout).build();

		return clientBuilder.setDefaultRequestConfig(config).build();
	}
}
