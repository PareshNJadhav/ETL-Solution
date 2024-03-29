# Spark Project on OLAP and OLTP dataset

This project is primarily designed for processing data coming in from the sources i.e. OLTP and OLAP using Spark computation.

## Installation guide and IDE recommendation

Code is written in pycharm IDE with pyspark library and python version 3.7 


```bash
pip install pyspark
```


## Contributing

This project was a part of a learning exercise but you can fork it and add your own logic changes as per your requirements.

## What is the final outcome from the datasets?

1. Calculate number of Zips per city.
   = 1 city can have multiple zipcode so will calculate number of zipcode for each city

2. Calculate the number of distinct prescriber and their total transaction count for each city.
   = out of whole dataset of prescriber's we want distinct prescribers

3. Don't create a report for city if no prescriber are present for that city.

