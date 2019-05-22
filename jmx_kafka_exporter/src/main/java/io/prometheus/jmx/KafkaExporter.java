package io.prometheus.jmx;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.StringWriter;
import java.io.Writer;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.Future;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.yaml.snakeyaml.Yaml;


public class KafkaExporter {
  KafkaExporter(CollectorRegistry registry, final String configFile) {
    _registry = registry;
    File f = new File(configFile);
    try { 
      config = loadConfig((Map<String, Object>)new Yaml().load(new FileReader(f)));
      if (config.enabled) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.clientId);
        props.put(ProducerConfig.ACKS_CONFIG, config.acks);
        props.put(ProducerConfig.LINGER_MS_CONFIG, config.lingerMs);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        _producer = new KafkaProducer(props);

        KafkaProduceTask task = new KafkaProduceTask(_producer, _registry, config);
        timer = new Timer("kafka_jmx_producer_timer", true);
        timer.scheduleAtFixedRate(task, config.delayMs, config.periodMs);
      }
    } catch (FileNotFoundException fnfe) {
      // ignore, it will just not be enabled.
      System.out.println("Error in configuring KafkaExporter, it will be disabled");
      System.out.println(fnfe);
    }
  }

  private Config loadConfig(Map<String, Object> yamlConfig) {
    Config cfg = new Config();

    if (yamlConfig == null) {  // Yaml config empty, set config to empty map.
      yamlConfig = new HashMap<String, Object>();
    }

    if (yamlConfig.containsKey("kafka")) {
      cfg.enabled = true;
      Map<String, Object> kafkaConfig = (Map<String, Object>)yamlConfig.get("kafka");
      cfg.kafkaBootstrapServers = (String) kafkaConfig.get("bootstrap_servers");
      cfg.topicName = (String) kafkaConfig.get("topic_name");
      cfg.clientId = (String) kafkaConfig.get("client_id");
      cfg.acks = (String)kafkaConfig.get("acks");
      try {
        cfg.delayMs = Long.valueOf((Integer)kafkaConfig.get("delay"));
      } catch (NumberFormatException e) {} // use default
      try {
        cfg.periodMs = Long.valueOf((Integer)kafkaConfig.get("period"));
      } catch (NumberFormatException e) {} // use default
      try {
        cfg.lingerMs = Long.valueOf((Integer)kafkaConfig.get("linger"));
      } catch (NumberFormatException e) {} // use default
    }
    return cfg;
  }


  private static class Config {
    String kafkaBootstrapServers = "";
    String clientId = "";
    String topicName = "";
    String acks = "all";
    boolean enabled = false;
    long delayMs = 1000;
    long periodMs = 60000;
    long lingerMs = 100;
  }

  public class KafkaProduceTask extends TimerTask {
    public KafkaProduceTask(KafkaProducer producer, CollectorRegistry registry, Config config) {
      _producer = producer;
      _registry = registry;
      _config = config;
    }

    public void run() {
      // crudely track our timing
      long start_ms = System.currentTimeMillis();
      // collect all futures
      List<Future<RecordMetadata>> futures = new LinkedList<Future<RecordMetadata>>();

      // ask the registry for all of the metric families
      Enumeration<Collector.MetricFamilySamples> mfs = _registry.metricFamilySamples();
      while(mfs.hasMoreElements()) {
        Writer writer = new StringWriter();
        // to preserve the TYPE and HELP metadata, we will produce a kafka message for each metricFamilySamples
        try {
          Collector.MetricFamilySamples metricFamilySamples = mfs.nextElement();
          writer.write("# HELP ");
          writer.write(metricFamilySamples.name);
          writer.write(' ');
          writeEscapedHelp(writer, metricFamilySamples.help);
          writer.write('\n');

          writer.write("# TYPE ");
          writer.write(metricFamilySamples.name);
          writer.write(' ');
          writer.write(typeString(metricFamilySamples.type));
          writer.write('\n');

          for (Collector.MetricFamilySamples.Sample sample: metricFamilySamples.samples) {
            writer.write(sample.name);
            if (sample.labelNames.size() > 0) {
              writer.write('{');
              for (int i = 0; i < sample.labelNames.size(); ++i) {
                if (i > 0) {
                  writer.write(",");
                }
                writer.write(sample.labelNames.get(i));
                writer.write("=\"");
                writeEscapedLabelValue(writer, sample.labelValues.get(i));
                writer.write("\"");
              }
            }
            if (sample.labelNames.size() > 0) {
              writer.write(',');
            } else {
              writer.write('{');
            }
            writer.write("host");
            writer.write("=\"");
            writeEscapedLabelValue(writer, getHostName());
            writer.write("\",");
            writer.write("os");
            writer.write("=\"");
            writeEscapedLabelValue(writer, System.getProperty("os.name"));
            writer.write("\"");
            writer.write('}');
            writer.write(' ');
            writer.write(Collector.doubleToGoString(sample.value));
            if (sample.timestampMs != null){
              writer.write(' ');
              writer.write(sample.timestampMs.toString());
            }
            writer.write('\n');
          }
        } catch(IOException e) {
          continue;
        }

        ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(_config.topicName, null, writer.toString());
        Future<RecordMetadata> metadata = _producer.send(record);
        futures.add(metadata);
      }

      int count = 0;
      for (Future<RecordMetadata> md : futures) {
        try {
          RecordMetadata m = md.get();
          count++;
        }
        catch (ExecutionException e) {
          System.out.println("Error in sending record");
          System.out.println(e);
        }
        catch (InterruptedException e) {
          System.out.println("Error in sending record");
          System.out.println(e);
        }
      }
      long end_ms = System.currentTimeMillis();
      System.out.println("Sent " + count + " metrics in " + (end_ms - start_ms) + " millisecs");
    }

    private String getHostName() {
      try {
        return InetAddress.getLocalHost().getHostName();
      } catch (Exception e) {
        return getHostNameFromEnv();
      }
    }

    private String getHostNameFromEnv() {
      // try environment properties.
      String host = System.getenv("COMPUTERNAME");
      if (host == null) {
        host = System.getenv("HOSTNAME");
      }
      if (host == null) {
        host = System.getenv("HOST");
      }
      return host;
    }


    private void writeEscapedHelp(Writer writer, String s) throws IOException {
      for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);
        switch (c) {
        case '\\':
          writer.append("\\\\");
          break;
        case '\n':
          writer.append("\\n");
          break;
        default:
          writer.append(c);
        }
      }
    }

    private void writeEscapedLabelValue(Writer writer, String s) throws IOException {
      for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);
        switch (c) {
        case '\\':
          writer.append("\\\\");
          break;
        case '\"':
          writer.append("\\\"");
          break;
        case '\n':
          writer.append("\\n");
          break;
        default:
          writer.append(c);
        }
      }
    }

    private String typeString(Collector.Type t) {
      switch (t) {
      case GAUGE:
        return "gauge";
      case COUNTER:
        return "counter";
      case SUMMARY:
        return "summary";
      case HISTOGRAM:
        return "histogram";
      default:
        return "untyped";
      }
    }

    private KafkaProducer _producer;
    private CollectorRegistry _registry;
    private Config _config;
  }

  private CollectorRegistry _registry;
  private KafkaProducer _producer;
  private Config config;
  private Timer timer;

  private synchronized void waitMethod() {

		while (true) {
			try {
				this.wait(2000);
			} catch (InterruptedException e) {
        e.printStackTrace();
			}
		}
	}

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: KafkaExporter <yaml configuration file>");
      System.exit(1);
    }

    new BuildInfoCollector().register();
    new JmxCollector(new File(args[0])).register();
    KafkaExporter ke = new KafkaExporter(CollectorRegistry.defaultRegistry, args[0]);
    ke.waitMethod();

  }
}
