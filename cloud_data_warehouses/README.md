# Sparkify ETL process
## Purpose
The sparkify ETL process pulls data from udacity stored in S3. This data provides user listening information which will provide useful insight like answering the questions below.  
**Which day and time of day users most frequently listen to music on the app?** This information will assist the advertizing department in knowing what to charge companies for running ads an the app during different times of the week.
**Which locations have the most listeners?** The advertizing department can use this information to target certain locations.

## Table schema Design
A start schema is used with a Fact table called `songplays` and dimension tables called `users`, `songs`, `artists`, and `time`. This schema design provides the data analysts and data scientists the ability to efficiently pull the data based on there specific question without having to query all the data contained within the staging tables.