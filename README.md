# Introduction
This project creates a data lakehouse on AWS in an S3 bucket to store sensor data and customer data used for training a machine learning model downstream.  This data lakehouse is an improved version of a data lake with an additional SQL metadata layer and filtered zones.

# Details
Data is stored as JSON within the lakehouse and queried using Athena.
Glue jobs perform ETL's to first pull raw data from source, and then to more filtered and curated zones within the lakehouse.  The zones are separated into landing, trusted, and curated folders.