/**
 * Copyright Ambud Sharma
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

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

import com.google.common.base.Splitter;
import com.srotya.sidewinder.core.rpc.Ack;
import com.srotya.sidewinder.core.rpc.BatchData;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Point.Builder;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc.WriterServiceBlockingStub;

import io.grpc.ManagedChannelBuilder;

/**
 * Simple influx client
 *
 */
public class SidewinderClient {

	private static final Splitter TAG = Splitter.on('=');
	private static final Splitter SPACE = Splitter.on(Pattern.compile("\\s+"));
	private static final Splitter COMMA = Splitter.on(',');
	private static final Splitter NEWLINE = Splitter.on('\n');
	private static final int LENGTH_OF_MILLISECOND_TS = 13;
	private static final Logger logger = Logger.getLogger(SidewinderClient.class.getName());

	public static void main(String[] args) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException,
			ClientProtocolException, IOException, InterruptedException {
		int _THREAD = 8;
		ThreadPoolExecutor es = new ThreadPoolExecutor(_THREAD, _THREAD, Integer.MAX_VALUE, TimeUnit.SECONDS,
				new ArrayBlockingQueue<>(100));

		BufferedReader reader = new BufferedReader(
				new InputStreamReader(new GZIPInputStream(new FileInputStream(args[0]))));

		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setMaxTotal(_THREAD);
		final GenericObjectPool<WriterServiceBlockingStub> pool = new GenericObjectPool<>(
				new PooledObjectFactory<WriterServiceBlockingStub>() {

					@Override
					public PooledObject<WriterServiceBlockingStub> makeObject() throws Exception {
						return new DefaultPooledObject<WriterServiceGrpc.WriterServiceBlockingStub>(
								WriterServiceGrpc.newBlockingStub(
										ManagedChannelBuilder.forAddress(args[1], 9928).usePlaintext(true).build()));
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
		try {
			Iterable<String> splits = NEWLINE.splitToList(payload);
			for (String split : splits) {
				List<String> parts = SPACE.splitToList(split);
				if (parts.size() < 2 || parts.size() > 3) {
					// invalid datapoint => drop
					continue;
				}
				long timestamp = System.currentTimeMillis();
				if (parts.size() == 3) {
					timestamp = Long.parseLong(parts.get(2));
					if (parts.get(2).length() > LENGTH_OF_MILLISECOND_TS) {
						timestamp = timestamp / (1000 * 1000);
					}
				} else {
					logger.info("Bad datapoint timestamp:" + parts.size());
				}
				List<String> key = COMMA.splitToList(parts.get(0));
				String measurementName = key.get(0);
				Set<Tag> tTags = new HashSet<>();
				for (int i = 1; i < key.size(); i++) {
					// Matcher matcher = TAG_PATTERN.matcher(key[i]);
					// if (matcher.find()) {
					// tTags.add(Tag.newBuilder().setTagKey(matcher.group(1)).setTagValue(matcher.group(2)).build());
					// }

					List<String> s = TAG.splitToList(key.get(i));
					tTags.add(Tag.newBuilder().setTagKey(s.get(0)).setTagValue(s.get(1)).build());
				}
				List<Tag> tags = new ArrayList<>(tTags);
				List<String> fields = COMMA.splitToList(parts.get(1));
				Builder builder = Point.newBuilder();
				builder.setDbName(dbName);
				builder.setMeasurementName(measurementName);
				builder.addAllTags(tags);
				builder.setTimestamp(timestamp);
				for (String field : fields) {
					String[] fv = field.split("=");
					String valueFieldName = fv[0];
					if (!fv[1].endsWith("i")) {
						double value = Double.parseDouble(fv[1]);
						builder.addValueFieldName(valueFieldName);
						builder.addValue(Double.doubleToLongBits(value));
						builder.addFp(true);
					} else {
						fv[1] = fv[1].substring(0, fv[1].length() - 1);
						long value = Long.parseLong(fv[1]);
						builder.addValueFieldName(valueFieldName);
						builder.addValue(value);
						builder.addFp(false);
					}
				}
				dps.add(builder.build());
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.fine("Rejected:" + payload);
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
