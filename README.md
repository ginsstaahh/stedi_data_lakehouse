# Introduction
This project creates a data lakehouse on AWS in an S3 bucket to store sensor and customer data from a fitness app, and prepares data for a machine learning model.  The data lakehouse is an improved version of a data lake with an additional SQL metadata layer and filtered zones.  This project was created for Udacity's Data Engineering Nanodegree.  After completing the Nanodegree, additional work was done to improve efficiency and automation; Glue jobs are changed to use Scala and the jobs are now sequenced using Airflow.