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
