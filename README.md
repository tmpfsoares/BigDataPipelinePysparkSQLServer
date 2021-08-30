# Table of Contents 
1. [Introduction](README.md#Introduction)
2. [Requirements](README.md#Requirements)
3. [Data Generation](README.md#Data%20Generation)
4. [Process Overview](README.md#Process%20Overview)
5. [Trial Instructions](README.md#Trial%20Instructions)

# Introduction
**Big Data pipeline using Python, Pyspark, SQL Server, Hive, Apache Hadoop with data lineage**

This is a project created with the goal of serving as a milestone that finalizes a period of self-study with the purpose of becoming a Data Engineer. 
Having past experience as a Data Analyst, I decided to use a few technologies with which I have worked before professionally such as SQL Server and Power BI.

This project consists of a process that encompasses two Big Data pipelines that ultimately work as one single Big Data process. 
For this project, I have created a dummy transactional database hosted on SQL Server for a fictional company called CoolWearPT, this would be a company that would be selling and exporting clothes within the European Union.
The goal here is to pull gigabytes or terabytes of data from a production database and send it on a pipeline to Hive using Python, and then from Hive apply calculations to the immense ammount of data and obtain aggregated datasets that are to be sent back to SQL Server but now hosted on a data mart for visualization purposes on Power BI.
As we do not want to transfer data unnecessarily this process was created using a data lineage approach that allows to keep track of what data needs to be transferred, thus avoiding a full load of the data lake table.

# Requirements

**Languages** 
* Python 3.8
* T-SQL

**Data Frameworks and RDBMS**
* SQL Server
* Apache Hadoop 3
* Apache Hive 3
* PySpark 3

**Orchestration tool**
* Airflow

**Visualization tool**
* Power BI


