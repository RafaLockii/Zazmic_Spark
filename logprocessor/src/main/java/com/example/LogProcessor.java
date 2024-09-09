package com.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class LogProcessor {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Log Processor")
                .master("local[*]") //This should change when necessary
                .getOrCreate();

        JavaRDD<String> logFile = spark.read()
                .textFile("/Users/rafaelreis/Documents/Jobs/Codes/Projects/Zazmic_Spark_Java/logprocessor/logs/sample_log.txt") //This should change when necessary
                .javaRDD();

        JavaRDD<LogEntry> logEntries = logFile.map(LogProcessor::parseLogEntry)
                .filter(Optional::isPresent)
                .map(Optional::get);

        Dataset<Row> logDF = spark.createDataFrame(logEntries, LogEntry.class);

        // Hourly and Daily Aggregation
        Dataset<Row> hourlyAggregation = logDF.groupBy(functions.window(logDF.col("timestamp"), "1 hour")).count();
        Dataset<Row> dailyAggregation = logDF.groupBy(functions.window(logDF.col("timestamp"), "1 day")).count();
        Dataset<Row> userActionAggregation = logDF.groupBy("userId", "action").count();

        // Top 5 most frequent ERROR messages
        Dataset<Row> errorLogs = logDF.filter(logDF.col("logLevel").equalTo("ERROR"));
        Dataset<Row> topErrorMessages = errorLogs.groupBy("details")
                .count()
                .orderBy(functions.desc("count"))
                .limit(5);

        // Average time between consecutive ERROR occurrences
        WindowSpec windowSpec = Window.partitionBy("details").orderBy("timestamp");
        Dataset<Row> errorWithLag = errorLogs.withColumn("prevTimestamp", functions.lag("timestamp", 1).over(windowSpec));
        Dataset<Row> avgTimeBetweenErrors = errorWithLag
                .withColumn("timeDiff", functions.unix_timestamp(functions.col("timestamp")).minus(functions.unix_timestamp(functions.col("prevTimestamp"))))
                .groupBy("details")
                .agg(functions.avg("timeDiff").alias("avgTimeBetweenErrors"));

        // Detecting user sessions
        WindowSpec userWindowSpec = Window.partitionBy("userId").orderBy("timestamp");
        Dataset<Row> sessionizedDF = logDF
                .withColumn("prevTimestamp", functions.lag("timestamp", 1).over(userWindowSpec))
                .withColumn("sessionDuration", functions.when(functions.col("prevTimestamp").isNotNull(),
                        functions.unix_timestamp(functions.col("timestamp")).minus(functions.unix_timestamp(functions.col("prevTimestamp"))))
                        .otherwise(0))
                .filter("sessionDuration <= 1800") // 30 minutes
                .groupBy("userId")
                .agg(functions.count("*").alias("sessionCount"),
                     functions.avg("sessionDuration").alias("avgSessionDuration"));

        // Caching and partitioning
        logDF.cache();
        logDF.repartition(10);

        // Print results to console
        hourlyAggregation.show();
        dailyAggregation.show();
        userActionAggregation.show();
        topErrorMessages.show();
        avgTimeBetweenErrors.show();
        sessionizedDF.show();

        spark.stop();
    }

    public static Optional<LogEntry> parseLogEntry(String logLine) {
        try {
            String[] parts = logLine.split(" ");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime timestamp = LocalDateTime.parse(parts[0] + " " + parts[1], formatter);
            String logLevel = parts[2];
            String userId = parts[3];
            String action = parts[4];
            String details = parts.length > 5 ? parts[5] : "";
            return Optional.of(new LogEntry(timestamp, logLevel, userId, action, details));
        } catch (Exception e) {
            System.err.println("Failed to parse log line: " + logLine);
            return Optional.empty();
        }
    }
}