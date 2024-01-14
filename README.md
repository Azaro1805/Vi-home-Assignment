# Data Engineering Assignment (pySpark)

## Table of Contents
1. [Assumptions](#assumptions)
2. [Objectives](#objectives)
3. [AWS Deployment](#aws-deployment)
4. [Remarks](#remarks)

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
   

## Objectives
1. **Compute the Average Daily Return:**
   - Compute the average daily return of all stocks for every date.
   - Print the results to the screen.

2. **Identify Most Frequently Traded Stock:**
   - Determine the stock traded most frequently, measured by closing price * volume, on average.

3. **Find the Most Volatile Stock:**
   - Identify the most volatile stock as measured by the annualized standard deviation of daily returns.
   - Output the result with the ticker and standard deviation.

4. **Top Three 30-day Return Dates:**
   - Find the top three 30-day return dates (% increase in closing price compared to the closing price 30 days prior).
   - Present ticker and date combinations.

## AWS Deployment

1. Results Storage in S3 Bucket
   - Results and intermediate data are stored in the [S3 Bucket](https://s3.console.aws.amazon.com/s3/buckets/aws-glue-home-assignment-or-azar?region=us-east-1&bucketType=general&tab=objects).

2. Glue Catalog Table Mapping
   - Glue Catalog Table mapping for each result file is created in the [Glue Catalog Database](https://us-east-1.console.aws.amazon.com/glue/home?region=us-east-1#/v2/data-catalog/databases/view/aws-glue-home-assignment-or-azar?catalogId=249751718460).

   - Average Daily Return Table:**
      - Partitioning:
         - Partitioned by year.

   - Average Frequently Traded Stock Table:**
      - **Partitioning:
         - Partitioned by the first letter of the stock name.

   - Most Volatile Stock Table:**
      - **Partitioning:
         - Partitioned by year.

   - 30-Day Return Dates Table:**
      - Partitioning:
         - Partitioned by year, month, and day.

## 3. Athena Querying
   - Make results queryable from [Athena](https://us-east-1.console.aws.amazon.com/athena/home?region=us-east-1#/query-editor).

### Remarks
1. add here

