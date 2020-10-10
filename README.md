
# Data Fabric Configuration

1) Create Stream and topics with public permmissions in Data Fabric (from the console):

Stream name: /sensor
Topic name: metric (receive telemetries from sensors)
Topic name: action (receive action to forword to devices)

```
maprcli stream delete -path /sensor
maprcli stream create -path /sensor 
maprcli stream edit -path /sensor -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /sensor -topic metric
maprcli stream topic create -path /sensor -topic action
```

2) Create database table with public permmissions in Data Fabric (from the console):

Table name: /metrics 
Table type: json

```
maprcli table create -path /metrics  -tabletype json
```

# MOSQUITTO (MQTT server):
Download from https://mosquitto.org/download/
Installation on MacOS : brew install mosquitto
Start Mosquitto (MQTT server) 

```
/usr/local/sbin/mosquitto -c /usr/local/etc/mosquitto/mosquitto.conf
```

# Java classes (applications)

##  MQTT Bridge ( MQTT > Kafka and Kafka > MQTT 

> org.hpe.bridge.MQTTBridge


##  Telemetry Consumer: 
Consume Kafka message to Populate Data Fabric Table - Metrics

> org.hpe.telemetry.TelemetryConsumer


##  MQTT Device: 
Simmulate Device generating telemetries (publish), and consuming event (subcribe) - based on HiveMQ

> org.hpe.sensor.Device


## Kafka Stream 
windowing generating actions if threshold exceeded

```
mkdir /tmp/kafka-streams  //on client
```

> org.hpe.stream.WindowedMeric


## Gateway Configuration 
To configure Log compaction required by Kafka streaming (on cluster)

```
sudo maprcli cluster gateway set -dstcluster my.cluster.com -gateways <cldb IP address>
```

check with : maprcli cluster gateway resolve -dstcluster my.cluster.com




##  Query à utiliser dans Drill (install Drill from MEP 7.0.0 with the installer - check Drill box)

> select * from dfs.`/metrics`


##  Utilities

> org.hpe.df.utilities.DeleteDBRows  (Delete data into '/metrics' table)
> org.hpe.df.utilities.ViewDBRows - (List data into '/metrics' table)