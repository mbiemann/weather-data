# Weather Data

This project aims to answer some questions about weather data from csv files, using Python and Data Engineering.

There are three ways to get to the answers in this repository:

1. [Using Jupyter Notebook](#1-using-jupyter-notebook)

2. [Event-driven Solution in AWS](#2-event-driven-solution-in-aws)

3. [Batch Solution in AWS](#3-batch-solution-in-aws)

The questions to answer are:

- Which date was the hottest day?

- What was the temperature on that day?

- In which region was the hottest day?

The csv files used for test are in [./test-data](test-data) folder.

___

## 1. Using Jupyter Notebook

Check the notebook with the answers here: [./notebook/weather-with-pandas.ipynb](notebook/weather-with-pandas.ipynb).

This script use Python 3 and Pandas library to:

a. Read csv files and Union dataframes:
```python
df1 = pd.read_csv('filename1.csv')
df2 = pd.read_csv('filename2.csv')
df = df1.append(df2)
```

b. Convert csv to parquet:
```python
df.to_parquet('filename.parquet')
df = pd.read_parquet('filename.parquet')
```

c. Get the answers:
```python
# What was the temperature on that day?
df['ScreenTemperature'].max()
# Which date was the hottest day?
df[ df['ScreenTemperature'] == df['ScreenTemperature'].max() ]['ObservationDate']
# In which region was the hottest day?
df[ df['ScreenTemperature'] == df['ScreenTemperature'].max() ]['Region']
```

___

## 2. Event-driven Solution in AWS

The Python script using in Lambda Function can be found on folder [aws-event-driven](aws-event-driven).

![AWS Diagram Event-Driven](aws-event-driven/aws-diagram-event-driven.png)

___

## 3. Batch Solution in AWS

Python Script uses for this solution is [aws-batch/glue-script.py](aws-batch/glue-script.py).

![AWS Diagram Batch](aws-batch/aws-diagram-batch.png)
