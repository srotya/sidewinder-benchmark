# sidewinder-benchmark
Repository shows how to run a benchmark on Sidewinder

### Building this project

```
# Download the project using git or download source from Github
git clone https://github.com/srotya/sidewinder-benchmark.git
cd sidewinder-benchmark

# Maven compile
mvn clean package -DskipTests

# Check the artifacts
ls -lh ./target/*.jar

# (Optional) Copy artifact to tmp directory
cp ./target/sidewinder-benchmark*.jar /tmp/
```

```
Requires:
- Java 8
- Maven 3
```

### Generate Data
Influx has a project to generate data for use cases, download [Influx-Comparisons](https://github.com/influxdata/influxdb-comparisons) project to generate data and then running multi-threaded client to run ingestion. Please follow the instructions below:

```
# Download influx comparisons
go get github.com/influxdata/influxdb-comparisons/cmd/bulk_query_gen

# Go to project directory
cd ~/go/

# Run data gen
bin/bulk_data_gen -timestamp-start "2018-03-01T00:00:00Z" -timestamp-end "2018-03-01T04:00:00Z" -scale-var 10000 > /tmp/10k_4h.txt

# Compress data
gzip /tmp/10k_4h.txt
```

## Ingestion Benchmark
Switch to Sidewinder-benchmark directory or use the jar file generated above from tmp directory. To run the benchmark:

In one terminal, launch Sidewinder:

```
# Make sure sidewinder is up and running
java -Xms8g -Xmx8g -cp sidewinder-standalone-dist*.jar com.srotya.sidewinder.core.SidewinderServer server side.yaml
```
Sidewinder launch configurations are available in [sidewinder_confs](https://github.com/srotya/sidewinder-benchmark/blob/master/sidewinder_confs) directory of the repo

Another terminal, launch ingestion client (same or different node is up to you); note that having the client and server on same node reduces performance by up to 40% due to lack of CPU.

```
java -cp /tmp/sidewinder-benchmark*.jar <benchmark class name>
```

#### Influx Line Protocol Benchmark
Sidewinder supports [Influx Line Protocol](https://docs.influxdata.com/influxdb/v0.9/write_protocols/line/) is one mechnaism to ingest data into Sidewinder. To run Influx API based write benchmark, run the following command:

```
java -cp /tmp/sidewinder-benchmark*.jar com.srotya.sidewinder.clients.InfluxClientStandalone <number of threads> <localhost/address of sidewinder server> <ingestion data file>
```

e.g. run configuration on a Macbook Pro

```
java -cp /tmp/sidewinder-benchmark*.jar com.srotya.sidewinder.clients.InfluxClientStandalone 6 localhost /tmp/10k_4h.txt.gz
```

