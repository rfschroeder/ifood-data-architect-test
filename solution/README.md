# The solution

To accomplish the challenge I tried to use the best of Python and Spark to create a processing data pipeline where, together, they can do the best of each to guarantee data integrity with performance and being able to scale to infinity.

I choose other tools like Apache Airflow, Amazon EMR, AWS Glue, Amazon Athena and Metabase. Each one with a specific responsibility on process, such as:

Ingestion
- Apache Airflow + Amazon EMR

Catalog
- AWS Glue + Hive Metastore

Data consumption
- Amazon Athena

Data visualization
- Metabase

## Spark

Two different Spark applications built to import raw datasets from source storage **s3://ifood-data-architect-test-source** and generate trusted datasets based on reliable data from raw datasets.
The solution uses an EMR cluster in a production environment, but it can also be executed locally (standalone mode).

Thinking about the many decision changes we constantly make in data pipelines (changing data sources locations, files format, etc.), I design this application should be as configurable as possible to address these situations.

There are two kind of main-classes of this Spark application:
1. One application to import raw datasets to DataLake (from external datasources)
2. Another application to process new trusted tables from DataLake raw datasets 

> Being four main-classes to import raw datasets to DataLake and three classes to process new trusted datasets.

Each kind of main-class was designed to reuse as much code as possible, with a bit of inheritance e polymorphism.  

Each dataset has a Scala main-class to receive params and processing data according needs coming from Apache Airflow (it will be explained with more details below). 

For each job of importation raw dataset is possible to configure:
- Source dataset path
- Source dataset format
- Target dataset path
- Target dataset format

For each job of processing trusted data is possible to configure:
- Target dataset path
- Target dataset format

The file `spark/scripts/deploy.sh` build the Scala project and generate the jar file in `target/scala-2.11/ifood-data-architect-test-1.0.0.jar`. 
It also copies the jar file to bucket **s3://ifood-data-architect-test-resources-renan**. 

> All my three buckets mentioned in this doc are public and with ACL read-public. 

## Client mode

Run the application locally through spark-submit of your spark local installation.

**Examples:**

**Import raw consumer to DataLake:**

./spark-submit --class com.ifood.dataarchitect.etl.layer.raw.ImportRawConsumer --master local --deploy-mode client --driver-memory 2g --executor-memory 8g --num-executors 4 /path/to/ifood-data-architect-etl-raw-1.0.0.jar s3n://ifood-data-architect-test-source/consumer.csv.gz csv s3n://bucket/prefix/datalake/raw/layer/ parquet

**Import raw order to DataLake:**

./spark-submit --class com.ifood.dataarchitect.etl.layer.raw.ImportRawOrder --master local --deploy-mode client --driver-memory 8g --executor-memory 8g --num-executors 4 /path/to/ifood-data-architect-etl-raw-1.0.0.jar s3n://ifood-data-architect-test-source/order.json.gz json s3n://bucket/prefix/datalake/raw/layer/ parquet

**Import raw restaurant to DataLake:**

./spark-submit --class com.ifood.dataarchitect.etl.layer.raw.ImportRawRestaurant --master local --deploy-mode client --driver-memory 2g --executor-memory 8g --num-executors 4 /path/to/ifood-data-architect-etl-raw-1.0.0.jar s3n://ifood-data-architect-test-source/restaurant.csv.gz csv s3n://bucket/prefix/datalake/raw/layer/ parquet

**Import raw order status to DataLake:**

./spark-submit --class com.ifood.dataarchitect.etl.layer.raw.ImportRawOrderStatus --master local --deploy-mode client --driver-memory 2g --executor-memory 8g --num-executors 4 /path/to/ifood-data-architect-etl-raw-1.0.0.jar s3n://ifood-data-architect-test-source/status.json.gz json s3n://bucket/prefix/datalake/raw/layer/ parquet

**Process trusted order to DataLake:**

./spark-submit --class com.ifood.dataarchitect.etl.layer.trusted.ProcessTrustedOrder --master local --deploy-mode client --driver-memory 8g --executor-memory 8g --num-executors 4 /path/to/ifood-data-architect-etl-raw-1.0.0.jar s3n://bucket/prefix/datalake/trusted/layer/ parquet

**Process trusted order items to DataLake:**

./spark-submit --class com.ifood.dataarchitect.etl.layer.trusted.ProcessTrustedOrderItems --master local --deploy-mode client --driver-memory 8g --executor-memory 8g --num-executors 4 /path/to/ifood-data-architect-etl-raw-1.0.0.jar s3n://bucket/prefix/datalake/trusted/layer/ parquet

**Process trusted order status to DataLake:**

./spark-submit --class com.ifood.dataarchitect.etl.layer.trusted.ProcessTrustedOrderStatus --master local --deploy-mode client --driver-memory 8g --executor-memory 8g --num-executors 4 /path/to/ifood-data-architect-etl-raw-1.0.0.jar s3n://bucket/prefix/datalake/trusted/layer/ parquet

## Cluster mode

To ensure ETLs will be executed in the most automated and scalable way possible, I created a pipeline for executor of Spark jobs in an EMR cluster using Apache Airflow.

The files inside folder apache-airflow/dags should be copied to the Airflow home folder (location folder used with $AIRFLOW_HOME environment variable). the Airflow quick start has all necessary information to start building an local environment (https://airflow.apache.org/docs/stable/start.html).

- `airflow/dags/ifood_ingestion_pipeline_dag.py` represents the DAG;
- `airflow/dags/infrastructure/emr_cluster_settings.py` is a Python class which represents the EMR cluster settings (hardware and resources). There are some configs that should be replaced to run this pipeline with Airflow, they are:
    - aws_account_id
    - aws_region
    - ec2_key_pair
    - ec2_subnet_id
    - Is nice also change these three variables on `ifood_ingestion_pipeline_dag.py`:
        - spark_app_jar_location: `s3://path/to/spark/jar`
        - target_raw_datasets_location = `s3n://bucket/path/to/raw/layer`
        - target_trusted_datasets_location = `s3n://bucket/path/to/raw/trusted` 

The pipeline of Airflow DAG has a flow of eight main steps, they are:

1. Creates a cluster on EMR
    - One task to create a cluster on EMR
2. Adds four importation raw datasets jobs steps to cluster EMR
    - One task to add a step to import raw consumer dataset to DataLake
    - One task to add a step to import raw order dataset to DataLake
    - One task to add a step to import raw restaurant dataset to DataLake
    - One task to add a step to import raw order status dataset to DataLake
3. Monitors the four steps above
    - One task to sensor the EMR step to import raw consumer dataset to DataLake
    - One task to sensor the EMR step to import raw order dataset to DataLake
    - One task to sensor the EMR step to import raw restaurant dataset to DataLake
    - One task to sensor the EMR step to import raw order status dataset to DataLake
4. Waits for all above steps to be completed
    - One task to wait until all of above steps to be completed
5. Adds three process trusted datasets steps to cluster EMR
    - One task to add a step to process trusted order dataset to DataLake
    - One task to add a step to process trusted order items dataset to DataLake
    - One task to add a step to process trusted order status dataset to DataLake
6. Monitors the three steps above
    - One task to sensor the EMR step to process trusted order dataset to DataLake
    - One task to sensor the EMR step to process trusted order items dataset to DataLake
    - One task to sensor the EMR step to process trusted order status dataset to DataLake
7. Waiting for all above steps to be completed
    - One task to wait until all of above steps to be completed
8. Complete cluster on EMR
    - One task to terminate the cluster on EMR 

To better comprehension, the image above show the GraphView of DAG, showing the connections between the steps:

![image](https://user-images.githubusercontent.com/7605282/89822799-bdeeb780-db26-11ea-93f5-9d9368f4a827.png)

Another representation of DAG execution can be watched out in Gantt tab that shows each task leading time:

![image](https://user-images.githubusercontent.com/7605282/89834484-542bd900-db39-11ea-9f64-65dd33d4cf2b.png)

All the spark jobs was processed successfully by seven steps no Amazon EMR, according image below:  

![image](https://user-images.githubusercontent.com/7605282/89845523-1c328f00-db55-11ea-9245-4216dbcd3047.png)

I used these instances EMR:
- 1 master node - m5.4xlarge (ON DEMAND instance)<br>
- 1 core node - m5.4xlarge (ON DEMAND instance)<br>
- 1 master node - c5.2xlarge (SPOT instance)

With these scale rules:

Core:
- In a period of 5 minutes if metric `YARNMemoryAvailablePercentage` has average less or equal 15 percent will add 1 new instance to instance group
- In a period of 5 minutes if metric `ContainerPendingRatio` has average less or equal 0.75 will add 1 new instance to instance group
- In a period of 5 minutes if metric `HDFSUtilization` has average greater or equal 80 percent will add 1 new instance to instance group
- In a period of 5 minutes if metric `MemoryAvailableMB` has average less or equal 10000 (10 MiB) will add 1 new instance to instance group
- In a period of 5 minutes if metric `YARNMemoryAvailablePercentage` has average greater or equal 75 percent will remove 1 new instance to instance group

Task:
- In a period of 5 minutes if metric `YARNMemoryAvailablePercentage` has average less or equal 15 percent will add 1 new instance to instance group
- In a period of 5 minutes if metric `ContainerPendingRatio` has average less or equal 0.75 will add 1 new instance to instance group
- In a period of 5 minutes if metric `MemoryAvailableMB` has average less or equal 10000 (10 MiB) will add 1 new instance to instance group
- In a period of 5 minutes if metric `YARNMemoryAvailablePercentage` has average greater or equal 75 percent will remove 1 new instance to instance group

> The onlt difference between them is that task core nodes does not persist data on HDFS (only the core nodes), and is not necessary to manage this scaling rule on task instance groups. 

The expected result when DAG pipeline ends is four datasets copied to raw DataLake layer and new three analytical datasets created in trusted layer of DataLake.

All four raw datasets copied to the bucket **s3://ifood-data-architect-raw-layer-renan** which represents the storage of raw datasets of my DataLake:

![image](https://user-images.githubusercontent.com/7605282/89833643-c6032300-db37-11ea-9b14-6a1c997507a1.png)

All three analytical tables processed to bucket **s3://ifood-data-architect-trusted-layer-renan** which represents the storage of trusted datasets of my DataLake:

![image](https://user-images.githubusercontent.com/7605282/89833679-d3201200-db37-11ea-9886-8fee8a9fb563.png)   

As can be seen below, the files of trusted order dataset was stored partitioned by restaurant local date column:

![image](https://user-images.githubusercontent.com/7605282/89834073-825ce900-db38-11ea-9d6e-6018b9d7698f.png)

All the tables cataloged on Hive Metastore and placed in two databases on AWS Glue (raw_layer and trusted_layer), so its possible to query them with Amazon Athena.

![image](https://user-images.githubusercontent.com/7605282/89851792-b8fc2900-db63-11ea-86ad-0e0e9c6b69fb.png)

Schema of table `raw_layer.consumer`:

![image](https://user-images.githubusercontent.com/7605282/89851879-f6f94d00-db63-11ea-98b4-62a7e3d9ebf6.png)

Schema of table `raw_layer.order`:

![image](https://user-images.githubusercontent.com/7605282/89852017-4e97b880-db64-11ea-8368-8e49067e1b71.png)

Schema of table `raw_layer.restaurant`:

![image](https://user-images.githubusercontent.com/7605282/89852087-76871c00-db64-11ea-852e-4b4b7a9fbc78.png)

Schema of table `raw_layer.order_status`:

![image](https://user-images.githubusercontent.com/7605282/89852158-a0d8d980-db64-11ea-9729-688ce6fce889.png)

Schema of table `trusted_layer.order`:

![image](https://user-images.githubusercontent.com/7605282/89829253-a6b4c780-db30-11ea-91ee-894185af8824.png)

Schema of table `trusted_layer.order_items`:

![image](https://user-images.githubusercontent.com/7605282/89852323-f90fdb80-db64-11ea-9373-94956617b456.png)

Schema of table `trusted_layer.order_status`:

![image](https://user-images.githubusercontent.com/7605282/89852399-2066a880-db65-11ea-89a7-de5c00e701de.png)

> All the tables are external tables in Hive Metastore with location on S3.<br>
They are cataloged automatically by Spark application (making the use of crawlers on Glue not necessary anymore). 

![image](https://user-images.githubusercontent.com/7605282/89834984-36ab3f00-db3a-11ea-89ef-e1fc5d714fff.png)

As a data visualization tool I used Metabase that connects with Amazon Athena through a driver which makes it possible to consuming data from AWS Glue/S3 with SQL ANSI.

Configuration of Athena database as `DataLake`:

![image](https://user-images.githubusercontent.com/7605282/89848930-cca49100-db5d-11ea-87bc-e593cf8592ba.png)

Databases and tables of AWS Glue can be accessed in the same way of Amazon Athena console.
  
![image](https://user-images.githubusercontent.com/7605282/89848814-7b949d00-db5d-11ea-9b53-1932d13fd922.png)

![image](https://user-images.githubusercontent.com/7605282/89849510-0b871680-db5f-11ea-8f6e-1b5e4ea495ca.png)

I built a dashboard in Metabase to demonstrate some operational questions that can be answered about the user's consumption behavior, like:

- Which cities do users most order food per app?
- What products are most popular?
- What the day of week which people more order food?
- What time has the highest rate of order cancellations?  

![image](https://user-images.githubusercontent.com/7605282/89828527-8afcf180-db2f-11ea-82cc-0f5b11198b2f.png)