# KPI Reporting System - Spark Consumer

## CS 5614: Big Data Engineering Project [Team 4]

### Project Members:
1. Sarvesh Patil (sarveshpatil@vt.edu)
2. Ankit Parekh (ankitparekh@vt.edu)
3. Pranjal Ranjan (pranjalranjan@vt.edu)
4. Badhrinarayan Malolan (badhrinarayan@vt.edu)
5. Swati Lodha (swatil@vt.edu)

### Consumer Project Setup:

#### Pre-requisites:
1. Install Java 8 with `%JAVA_HOME%` system environment variable configured
   ![Alt text](docs/java_installation_verification.png?raw=true)
2. Install IntelliJ Idea Community Edition
3. (For Windows Users Only) Download `winutils.exe` from this link: https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe
   
   Save this file to `C:\hadoop\bin`. The folder should look like this:
   ![Alt text](docs/winutils.jpeg?raw=true)
   
   Once added, configure system environment variable `%HADOOP_HOME%` as `C:\hadoop`.


#### Steps to run the consumer:
1. Clone this repository from IntelliJ Idea IDE. (Create project with "Get from VCS" feature using HTTPS GIT URL)
   ![Alt text](docs/spark_consumer_clone.png?raw=true)
2. Check if the `%HADOOP_HOME%` variable is set in the IDE terminal as follows:

   ![Alt text](docs/hadoop_home.jpeg?raw=true)
3. Verify if Producer is running without any issues and sending data to the Kafka Topic.
4. Run the DimensionTablesGenerator.scala file once. This will generate CSV files inside the DimensionTables folder. Once these files are created, you do not have to run this step again.
5. Build and Run the sbt project (or KPIGenerator.scala) in the IDE.
