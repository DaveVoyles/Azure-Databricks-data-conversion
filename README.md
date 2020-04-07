# Incremental loading of parquet files in Azure Databricks with blob storage

## For manipulating data and learning ADB. Mostly .csv &amp; parquet work

#### AUTHOR: Dave Voyles | Microsoft | April-2020 | https://github.com/DaveVoyles/Azure-Databricks-data-conversion

---
### GOAL: Incremental loading of parquet files

  1) Take something that has "date time" and run a filter that filters to one day at a time.
  2) Then add that data to an existing parquet data set.


## Steps:

 1) Mount to Azure blob storage -- allows us to read/write. Mount point is: "/mnt/blobmount"
 2) Read .csv from from "/mnt/blobmount/example/data/users.csv"
 3) Write .csv file back to blob as parquet OR csv (in this example, parquet) to "/example/data/users/incremental" folder.
 NOTE: We are appending the current date, up to the minute, to prevent overwriting the existing parquet file
 4) Read back parquet file as data frame
 5) Filter df by between a start & end date
 NOTE: All transactions occur on the same day, so we filter by HOUR here ('03') to give us fewer results
 6) Append newly filtered DF to existing parquet file
 
 ---
## Conclusion
   We have seen that it is very easy to add data to an existing Parquet file. This works very well when you’re adding data - as opposed to updating or deleting existing records - to a cold data store (Amazon S3, for instance). 

   In the end, this provides a cheap replacement for using a database when all you need to do is offline analysis on your data. 
   You can add partitions to Parquet files, but you can’t edit the data in place.
 
 * SOURCE: https://stackoverflow.com/questions/28837456/updating-values-in-apache-parquet-file
 * SOURCE: http://aseigneurin.github.io/2017/03/14/incrementally-loaded-parquet-files.html
