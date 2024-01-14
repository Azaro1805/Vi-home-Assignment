# Data Engineering Assignment (pySpark)

## Table of Contents
1. [Assumptions](#assumptions)
2. [AWS Deployment](#aws-deployment)
3. [Remarks](#remarks)

## Assumptions
1. **Handling Missing Prices (Q1):**
   - If the closing price is null on a specific date for a given stock ticker, the approach is to use the closing price of the same ticker from the last available day.
   - The last available day includes only previous days and can extend beyond one day.
   - If previous values are not found (e.g., for the first row of the data), the daily return will be NaN.

2. **Calculation of Average Daily Return (Q2):**
   - When calculating the average daily return of all stocks for every date, I used the close price of the current date and the close price of the last available day.
   - The last available day is determined based on the approach mentioned in Q1.
         - Gaps between the current date and the last available day are considered as part of the average daily return.**
         - The division for calculating the average daily return is not explicitly specified, so it is assumed that it includes the number of days between the current date and the last available day.

3. **Volatility Measurement (Q3):**
   - The output should provide both the stock ticker and the associated standard deviation for a specific year.

4. **Handling Missing 30-Day Prior Closing Prices (Q4):**
           - In cases where the exact closing price from 30 days prior is unavailable, it is acceptable to omit the calculation of the percentage increase in closing price.
   

## AWS Deployment

1. **Results Storage in S3 Bucket:**
   - Results and intermediate data are stored in the [S3 Bucket](https://s3.console.aws.amazon.com/s3/buckets/aws-glue-home-assignment-or-azar?region=us-east-1&bucketType=general&tab=objects).

2. **Glue Catalog Table Mapping:**
   - Glue Catalog Table mapping for each result file is created in the [Glue Catalog Database](https://us-east-1.console.aws.amazon.com/glue/home?region=us-east-1#/v2/data-catalog/databases/view/aws-glue-home-assignment-or-azar?catalogId=249751718460).

   - **Average Daily Return Table:**
      - Partitioned by year.

   - **Average Frequently Traded Stock Table:**
      - Partitioned by the first letter of the stock name.

   - **Most Volatile Stock Table:**
      - Partitioned by year.

   - **30-Day Return Dates Table:**
      - Partitioned by year, month, and day.

3. **Athena Querying:**
   - Make results queryable from [Athena](https://us-east-1.console.aws.amazon.com/athena/home?region=us-east-1#/query-editor).

     
## Remarks

- **Efficient Data Organization and Flexibility:** 
  - Partitioning organizes data into subdirectories based on specific criteria, like time-based or categorical attributes, ensuring efficient organization and providing flexibility for granular data analysis.

- **Optimized Query Performance and Cost Efficiency:** 
  - Athena leverages partitions to optimize query performance by scanning only relevant data subsets, leading to cost-efficient data processing as users are billed based on the reduced amount of data scanned.
  
- **Organized Functions:**
  - Analysis-related functions are structured in distinct notebook cells, enhancing maintainability. The plan for the future involves moving these functions to a utility file.
  - Crawler-related utilities are organized into notebook cells, setting the stage for future consolidation into a dedicated controller for enhanced manageability.
   
- **Cost Optimization:**
  - The number of job nodes has been reduced from the default 5 to 2, ensuring a cost-effective setup. In the future, adjustments may be made based on evolving requirements or resource needs.
    
- ** Other: **
  - Rows with null values in the specified subset ("date" and "ticker") are removed, ensuring data integrity for subsequent analysis.
  - Results and intermediate data are stored in well-structured folders within the S3 Bucket, ensuring a clear and organized layout for easy navigation and future reference.
