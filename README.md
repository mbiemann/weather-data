# Weather Data

This project aims to answer some questions about weather data from csv files, using Python and Data Engineering.

There are three ways to get to the answers in this repository:

1. Using Jupyter Notebook

2. Event-driven Solution in AWS

3. Batch Solution in AWS

The questions to answer are:

- Which date was the hottest day?

- What was the temperature on that day?

- In which region was the hottest day?

There are some test csv files on [./test-data](test-data).



___

## 1. Using Jupyter Notebook

The script to get to the answers using Jupyter Notebook is in the folder [notebook](notebook/weather-with-pandas.ipynb).

___

## 2. Event-driven Solution in AWS

The Python script using in Lambda Function can be found on folder [aws-event-driven](aws-event-driven).

![AWS Diagram Event-Driven](aws-event-driven/aws-diagram-event-driven.png)

___

## 3. Batch Solution in AWS

Python Script uses for this solution is [aws-batch/glue-script.py](aws-batch/glue-script.py).

![AWS Diagram Batch](aws-batch/aws-diagram-batch.png)

___

