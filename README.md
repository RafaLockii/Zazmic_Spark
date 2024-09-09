# Log Processor

A simple log processing application using Apache Spark in Java. This project processes log files, performs various analyses, and outputs results.

## Features

- **Hourly and Daily Aggregation**: Aggregates log entries by hour and day.
- **Top 5 Most Frequent ERROR Messages**: Identifies the most common ERROR messages.
- **Average Time Between Consecutive ERROR Occurrences**: Calculates the average time between consecutive ERROR messages.
- **User Session Detection**: Analyzes user sessions based on log entries.

## Getting Started

These instructions will help you set up the project on your local machine.

### Prerequisites

- Java Development Kit (JDK) 8 or later
- Apache Maven
- Apache Spark

### Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/RafaLockii/Zazmic_Spark.git
   cd Zazmic_Spark/logprocessor
   ```

Ensure that Maven is installed, then build the project using:
  ```bash
   mvn clean install
   mvn exec:java -Dexec.mainClass="com.example.LogProcessor"
