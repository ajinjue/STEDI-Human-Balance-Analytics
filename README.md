# STEDI-Human-Balance-Analytics

## Project Introduction
In this project, I am acting as a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.
Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

## Project Environment
To develop a data lakehouse solution in the AWS cloud that curates the data for the machine learning model, we use the following tools and services:
- Python and Spark
- AWS Glue
- AWS Athena
- AWS S3

## Data Lakehouse Architecture
![image](https://github.com/ajinjue/STEDI-Human-Balance-Analytics/assets/100845693/9a1e81ea-1f07-4f69-b51f-82cd1066e29c)

## Project Data
STEDI has three JSON data sources to use from the Step Trainer. 
- customer
- step_trainer
- accelerometer <br/>

Check out the JSON data in the following folders in the Github repo linked: <br/>
https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter

The datasets have the following fields: <br/>
**customer:** serialnumber, sharewithpublicasofdate, birthday, registrationdate, sharewithresearchasofdate, customername, email, lastupdatedate, phone, sharewithfriendsasofdate <br/>
**step_trainer:** sensorReadingTime, serialNumber, distanceFromObject <br/>
**accelerometer:** timeStamp, user, x, y, z

## Requirements
To simulate the data coming from the various sources (data lake), I created an S3 bucket, 3 directories in the bucket, and copied the datasets in them. I used AWS Glue's Data Catalog to create Glue tables as customer_landing, step_trainer_landing, and accelerometer_landing zones.

## Program Files
**customer_landing.png:** It contains the result for querying the entire customer_landing table using AWS Athena Query Editor. Its content is showed below:

![image](https://github.com/ajinjue/STEDI-Human-Balance-Analytics/assets/100845693/09060de9-9200-4dbe-8f79-605d94cf71a3)

**accelerometer_landing.png:** It contains the result for querying the entire accelerometer_landing table using AWS Athena Query Editor. Its content is showed below:

![image](https://github.com/ajinjue/STEDI-Human-Balance-Analytics/assets/100845693/891b7c77-0082-47c4-bdc6-f25b28eacaca)

**Customer_Landing_to_Trusted.py:** It's the python script for the Glue job to sanitize the Customer data in the Landing zone. That's this job filters only Customer Records who agreed to share their data for research purposes and stores the resulting data the Trusted zone; from which the customer_trusted table is created.

**customer_trusted.png:** It contains the result for querying the entire customer_trusted table using AWS Athena Query Editor. Its content is showed below:

![image](https://github.com/ajinjue/STEDI-Human-Balance-Analytics/assets/100845693/5275d54b-2db6-490e-8f0e-37b6f0ad61c0)

The resulting customer trusted data has no rows where shareWithResearchAsOfDate is blank. This is shown below:

![image](https://github.com/ajinjue/STEDI-Human-Balance-Analytics/assets/100845693/2b770532-29dd-496a-a10a-d8f90d58cd5e)

**customer_landing.sql:** It's the equivalent SQL code to create the customer_landing table from within AWS Athena Query Editor. <br/>
**accelerometer_landing.sql:** It's the equivalent SQL code to create the accelerometer_landing table from within AWS Athena Query Editor. <br/>
**Accelerometer_Landing_to_Trusted.py:** It's the python script for the Glue job to sanitize the Accelerometer data in the Landing zone. That's this job stores only Accelerometer Readings from customers who agreed to share their data for research purposes in the Trusted zone. <br/>
**Customer_Trusted_to_Curated.py:** It's the python script for the Glue job that takes Customer data from the Trusted zone to the Curated zone after joining it with the Accelerometer data. <br/>
**Step_Trainer_Landing_to_Trusted.py:** It's the python script for the Glue job that reads the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called **step_trainer_trusted** which contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated). <br/>
**Step_Trainer_Trusted_to_Curated.py:** It's the python script for the Glue job which aggregates the table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called **machine_learning_curated**.





