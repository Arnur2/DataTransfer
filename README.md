# DataTransfer
Data tranfer task

```
Preparations
Basically that is all prepartions that I have done now to transfer data
I created both databases with the same table structure.
I create a partition in target_db for yesterday’s date.
I enable the dblink extension in target_db to connect to source_db
I connect to source_db using dblink inside a SQL script.
I transfer data in batches (1 million rows at a time) using OFFSET and LIMIT.
After each batch, I check how many rows were inserted and log the batch number. If any batch fails, I log which one caused the error.This helps me restart from the failed batch if needed. After the full transfer, I disconnect the dblink connection.
Tools and Technologies Used
PostgreSQL: The databases are PostgreSQL, so I use native SQL and PL/pgSQL.
dblink: Used to connect and fetch data from another PostgreSQL database. With it I can save the hassle of dealing with export files and finding the disk space , then transferring the files
PL/pgSQL: Allows me to write loops, handle errors, and control the transfer process.
Key Challenges or Considerations
Big data volume – I’m dealing with 100 million rows per day, so I split the transfer into batches.
Memory Limits: I don’t have a lot of a space on my pc
Transfer Speed: Moving 100 million rows can take 1–2 hours on my pc, so we may need to make batches larger or copy rows first into file and then in another db.
Data Integrity: We need to make sure no rows are lost.
Recovery: If something fails, I want to know where/which batch so I can fix it or just retry.
Possible perfection: With a dblink it is possible to easily automate this process with cron, airflow
Validation
I compare the number of rows in the recommendations table in both source_db and target_db
During the transfer, I log each batch number and how many rows were inserted.This helps to track progress and identify if any batch failed. If a batch fails, I catch the error, print the batch number and offset, and can restart the process from that point. Another option is to add a hash for each batch and check it.
```
