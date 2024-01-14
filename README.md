# Data Engineering Assignment (pySpark)

## Table of Contents
1. [Assumptions](#assumptions)
2. [Initial Variables Setup](#initial-variables-setup)
3. [AWS Deployment](#aws-deployment)
4. [Remarks](#remarks)
5. [Bonus - CloudFormation](#bonus---cloudformation)

## Assumptions
1. **Handling Missing Prices (Q1):**
   - If the closing price is null on a specific date for a given stock ticker, the approach is to use the closing price of the same ticker from the last available day.
   - The last available day includes only previous days and can extend beyond one day.
   - If previous values are not found (e.g., for the first row of the data), the daily return will be NaN.

2. **Calculation of Average Daily Return (Q2):**
   - When calculating the average daily return of all stocks for every date, I used the close price of the current date and the close price of the last available day.
   - The last available day is determined based on the approach mentioned in Q1.

3. **Result File Completeness Assumption:**
   - Assumed that saved files encompass the entire dataset with relevant fields, not limited to the top 1 or top 3 records.

4. **Cost Optimization Note:**
   - The number of nodes has been reduced from the default 10 to 2 for cost optimization. If time sensitivity for the home assignment grade arises, please revert to the default setting of 10 nodes.

## Initial Variables Setup
- **File Name:** `stock_prices.csv` is the required file in the S3 bucket (your bucket) under the "original_files" folder. If modified or changed, update the `file_name` variable accordingly.
- **Crawler Wait:** By default, the job doesn't wait for the crawler to complete (`wait_to_crawler = False`). Change this variable to `True` if you want the job to wait for the crawler's successful execution.

## AWS Deployment
1. **Glue Job:**
   - I made a Glue job with prints [Glue_Job](https://us-east-1.console.aws.amazon.com/gluestudio/home?region=us-east-1#/editor/job/aws-glue-home-assignment-or-azar/script)

2. **Results Storage in S3 Bucket:**
   - Results and intermediate data are stored in the [S3 Bucket](https://s3.console.aws.amazon.com/s3/buckets/aws-glue-home-assignment-or-azar?region=us-east-1&bucketType=general&tab=objects).

3. **Glue Catalog Table Mapping:**
   - Glue Catalog Table mapping for each result file is created in the [Glue Catalog Database](https://us-east-1.console.aws.amazon.com/glue/home?region=us-east-1#/v2/data-catalog/databases/view/aws-glue-home-assignment-or-azar-db?catalogId=249751718460). 
      
   Name: "aws-glue-home-assignment-or-azar-db"
      - **Average Daily Return Table:**
         - Partitioned by year.
      - **Average Frequently Table:**
         - Partitioned by the first letter of the stock name.
      - **Volatility Annualized Std Table:**
         - Partitioned by the first letter of the stock name.
      - **Stock Prices 30 Days of Return Table:**
         - Partitioned by year, month, and day.

4. **Athena Querying:**
   - Make results queryable from [Athena](https://us-east-1.console.aws.amazon.com/athena/home?region=us-east-1#/query-editor). (Make sure that the DB is: "aws-glue-home-assignment-or-azar-db")

## Remarks
- **Efficient Data Organization and Flexibility:** 
   - Partitioning organizes data into subdirectories based on specific criteria, like time-based or categorical attributes, ensuring efficient organization and providing flexibility for granular data analysis.

- **Optimized Query Performance and Cost Efficiency:** 
   - Athena leverages partitions to optimize query performance by scanning only relevant data subsets, leading to cost-efficient data processing as users are billed based on the reduced amount of data scanned.

- **Organized Functions:**
   - Analysis-related functions are structured in distinct cells, enhancing maintainability. The plan for the future involves moving these functions to a utility file.
   - Crawler-related utilities are organized into cells, setting the stage for future consolidation into a dedicated controller for enhanced manageability.

- **Cost Optimization:**
   - The number of job nodes has been reduced from the default 10 to 2, ensuring a cost-effective setup. In the future, adjustments may be made based on evolving requirements or resource needs.

- **Other:**
   - Null values in the specified subset ("date" and "ticker") are eliminated from the dataset to maintain data integrity for further analysis. The number of removed rows is logged for reference.
   - Results and intermediate data are stored in well-structured folders within the S3 Bucket, ensuring a clear and organized layout for easy navigation and future reference.

## Bonus - CloudFormation

I successfully deployed and tested a CloudFormation stack using a YAML file in my personal AWS environment. However, replication in the home assignment environment encountered IAM role and permissions issues. Despite multiple role and configuration adjustments, the errors persisted.

For clarity, I've provided a folder named "Cloud-Formation," containing the YAML file and a snapshot of the AWS environment post-deployment. This documentation aims to showcase my troubleshooting efforts in addressing the issues.

Given more time and access to IAM roles,  I would establish a stack resembling the configuration in my personal environment.
