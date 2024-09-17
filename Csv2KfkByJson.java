package org.example;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.kafka.clients.producer.*;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Csv2KfkByJson {
    private static String KAFKA_TOPIC = "topic-test";
    private static String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static String csvFilePath;
    private static String delimiter;
    // 字段列表（,分割）
    private static String fldListStr;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("请提供CSV文件路径和分隔符作为参数");
            System.out.println("usage: <0>文件全路径  <1>分隔符 <2>字段列表(,号分割) <3>topic名 <4>kafka服务器和端口列表");
            return;
        }

        csvFilePath = args[0];
        delimiter = args[1];
        fldListStr = args[2];
        KAFKA_TOPIC = args[3];
        KAFKA_BOOTSTRAP_SERVERS = args[4];

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        String[] fldList = fldListStr.split(",");
        CsvSchema csvSchema = buildSchema(fldList, delimiter.charAt(0));

        try {
            WatchService watchService = FileSystems.getDefault().newWatchService();
            Path path = Paths.get(csvFilePath).getParent();
            path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

//            long lastPosition = 0;
            long lastPosition = readLastPosition(csvFilePath); // 读取特定文件的上次处理位置

            while (true) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                        if (event.context().toString().equals(Paths.get(csvFilePath).getFileName().toString())) {
                            lastPosition = processCSVFile(producer, csvSchema, lastPosition);
                            writeLastPosition(csvFilePath, lastPosition); // 更新特定文件的位置
                        }
                    }
                }
                key.reset();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static long processCSVFile(Producer<String, String> producer,
                                       CsvSchema csvSchema,
                                       long lastPosition) throws IOException {
        File csvFile = new File(csvFilePath);
//        CsvSchema csvSchema = CsvSchema.emptySchema().withHeader().withColumnSeparator(delimiter.charAt(0));
        try (RandomAccessFile raf = new RandomAccessFile(csvFile, "r")) {
            raf.seek(lastPosition);
            String line;
            while ((line = raf.readLine()) != null) {
                line = line.trim();
                // 忽略空行
                if (line.trim().isEmpty())
                    continue;

                try {
                    System.out.println(line);
                    String jsonLine = convertCsvToJsonByLine(line, csvSchema);
                    if (null == jsonLine || "null".equals(jsonLine) || jsonLine.length() == 0)
                        continue;
                    System.out.println(jsonLine);
                    producer.send(new ProducerRecord<>(KAFKA_TOPIC, jsonLine));
                } catch (JsonProcessingException e) {
                    // 忽略无法解析的行，但打印日志以供调试
                    System.err.println("Failed to parse line: " + line);
                    e.printStackTrace();
                }
            }
            lastPosition = raf.getFilePointer();
        }
        return lastPosition;
    }


    public static String convertCsvToJsonByLine(String csvline, CsvSchema csvSchema) throws IOException {
        CsvMapper csvMapper = new CsvMapper();

        // 读取 CSV 内容
        MappingIterator<Map<String, String>> it = csvMapper.readerFor(Map.class)
                .with(csvSchema)
                .readValues(new StringReader(csvline));

        // 创建 JSON Mapper
        ObjectMapper jsonMapper = new ObjectMapper();

        // 遍历每一行 CSV 数据并转换成 JSON
        if (it.hasNext()) {
            Map<String, String> rowAsMap = it.next();
            String jsonString = jsonMapper.writeValueAsString(rowAsMap);
            return jsonString;
        }
        return null;
    }

    public static CsvSchema buildSchema(String[] fldList, char sep) {
        CsvSchema.Builder schemaBuilder = CsvSchema.builder();
        for (String field : fldList) {
            schemaBuilder.addColumn(field);
        }
        CsvSchema csvSchema = schemaBuilder.build().withColumnSeparator(sep).withSkipFirstDataRow(false);
        return csvSchema;
    }


    // 文件位置记录，支持断点
    private static long readLastPosition(String filePath) {
        String positionFilePath = filePath + ".pos"; // 每个文件对应一个位置文件
        try (BufferedReader reader = new BufferedReader(new FileReader(positionFilePath))) {
            String line = reader.readLine();
            return line != null ? Long.parseLong(line) : 0; // 返回上次位置或0
        } catch (IOException e) {
            return 0; // 文件不存在时返回0
        }
    }

    private static void writeLastPosition(String filePath, long position) {
        String positionFilePath = filePath + ".pos"; // 每个文件对应一个位置文件
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(positionFilePath))) {
            writer.write(Long.toString(position)); // 写入当前处理的位置
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
