import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from os import path
import boto3
import time


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

############################### initial vars ###############################

file_name = f"stock_prices.csv"  # this file must be in the S3 bucket (my bucket) in folder original_files/
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
glue_client = boto3.client("glue")
crawler_name = "aws-glue-home-assignment-or-azar_crawler"
glue_catalog_db_name = "aws-glue-home-assignment-or-azar-db"
folder_path_in_s3 = "s3://aws-glue-home-assignment-or-azar/"
wait_to_crawler = False

############################### utils functions ###############################


def calculate_x_days_return_value(stock_prices, days_of_return=30):
    window_spec = Window().partitionBy("ticker").orderBy("date")
    stock_prices = stock_prices.withColumn(f"close_{days_of_return}_days", lag("close").over(window_spec))
    stock_prices = calculate_return_rate(
        stock_prices, "final_close", f"close_{days_of_return}_days", f"return_rate_{days_of_return}_days"
    )
    stock_prices = stock_prices.select("ticker", "date", f"return_rate_{days_of_return}_days")
    return stock_prices


def organize_df(stock_prices_df):
    # Convert the columns types and clean unused data
    try:
        stock_prices_df = stock_prices_df.withColumn("date", to_date(col("date"), "MM/dd/yyyy"))
        stock_prices_df = stock_prices_df.withColumn("close", col("close").cast("float"))
        stock_prices_df = stock_prices_df.withColumn("volume", col("volume").cast("float"))
        stock_prices_df = stock_prices_df.withColumn("ticker", col("ticker").cast("string"))
        filtered_stock_prices_df = stock_prices_df.select("date", "ticker", "close", "volume").na.drop(
            subset=["date", "ticker"]
        )
        number_of_rows_dropped = (
            stock_prices_df.select("date", "ticker").filter("date IS NULL OR ticker IS NULL").count()
        )
        print(f"{number_of_rows_dropped} rows dropped due to nan values in columns: date, ticker ")
        return filtered_stock_prices_df
    except:
        raise print(
            "Failed to convert the columns types and clean unused data, continue without it, please check the data Error:{e}"
        )


def fill_null_close_values(stock_prices_df):
    window_spec_ticker_date = Window().partitionBy("ticker").orderBy("date")
    stock_prices_with_all_values_df = stock_prices_df.filter(stock_prices_df["close"].isNotNull())
    stock_prices_with_all_values_df = stock_prices_with_all_values_df.withColumn(
        "prev_date", lag("date").over(window_spec_ticker_date)
    )
    stock_prices_with_all_values_df = stock_prices_with_all_values_df.withColumn(
        "prev_close", lag("close").over(window_spec_ticker_date)
    )
    stock_prices_with_nan_values_df = stock_prices_df.filter(
        stock_prices_df["close"].isNull() | isnan(stock_prices_df["close"])
    )
    stock_prices_with_nan_values_df.createOrReplaceTempView("stock_prices_with_nan_values")
    stock_prices_with_all_values_df.createOrReplaceTempView("stock_prices_with_all_values")
    days_difference_df = spark.sql(
        """
        SELECT 
            nan_values.*,
            all_values.date AS prev_date,
            all_values.close AS prev_close,
            DATEDIFF(nan_values.date, all_values.date) AS days_difference
        FROM stock_prices_with_nan_values nan_values
        LEFT JOIN stock_prices_with_all_values all_values ON nan_values.ticker = all_values.ticker 
            AND nan_values.date >= all_values.date
        """
    )

    window_spec_days_difference = Window.partitionBy("nan_values.ticker", "nan_values.date").orderBy("days_difference")

    result_df_filtered = (
        days_difference_df.withColumn("min_days_difference", min("days_difference").over(window_spec_days_difference))
        .filter("days_difference = min_days_difference")
        .drop("min_days_difference", "days_difference")
    )
    stock_prices_with_all_values_df = stock_prices_with_all_values_df.union(result_df_filtered)
    stock_prices_with_all_values_df = stock_prices_with_all_values_df.withColumn(
        "final_close", coalesce("close", "prev_close")
    )
    return stock_prices_with_all_values_df


def calculate_return_rate(stock_prices, current_price_col_name, prev_price_col_name, new_col_name):
    stock_prices = stock_prices.withColumn(
        new_col_name, ((col(current_price_col_name) - col(prev_price_col_name)) * (100 / col(prev_price_col_name)))
    )
    return stock_prices


def calculate_frequently(stock_prices):
    stock_prices = stock_prices.withColumn("frequently", col("final_close") * col("volume"))
    return stock_prices.orderBy("date")


def calculate_volatile_by_annualized_std(stock_prices, trading_days_per_year=252):
    volatility_df = stock_prices.groupBy("ticker").agg(stddev("return_rate").alias("standard deviation"))

    # Calculate annualized standard deviation using the formula: SD * sqrt(number of trading days per year)
    volatility_df = volatility_df.withColumn(
        "annualized_std", col("standard deviation") * sqrt(lit(trading_days_per_year))
    )
    volatility_df = volatility_df.orderBy("annualized_std", ascending=False)
    return volatility_df.orderBy("annualized_std", ascending=False)


############################### crawler functions ###############################


def get_crawler_if_exists(crawler_name, verbose=True):
    try:
        response = glue_client.get_crawler(Name=crawler_name)
        if verbose:
            print("Crawler already exists.")
        return response
    except glue_client.exceptions.EntityNotFoundException:
        return False


def create_new_crawler(crawler_name, Iam_role, glue_catalog_db_name, s3_paths):
    new_crawler = {
        "Name": crawler_name,
        "Role": Iam_role,
        "DatabaseName": glue_catalog_db_name,
        "Targets": {"S3Targets": [{"Path": path} for path in s3_paths]},
    }
    try:
        response = get_crawler_if_exists(crawler_name)
        if response == False:
            create_response = glue_client.create_crawler(**new_crawler)
            print("Crawler created successfully.")
            response = get_crawler_if_exists(crawler_name, verbose=False)
        return response
    except Exception as e:
        raise Exception(f"Tried to create a new crawler and failed, Error: {e}")


def start_crawler(crawler_name, crawler):
    if crawler["Crawler"]["State"] == "READY":
        try:
            glue_client.start_crawler(Name=crawler_name)
            print("Crawler started successfully.")
        except Exception as e:
            raise Exception(f"The crawler is ready but can’t start successfully, Error: {e}")
    else:
        raise Exception(f"The crawler is not ready to run, please wait and try again")


def add_data_source_to_glue_crawler(
    glue_client, crawler_name, data_source_path_list, return_response=False, verbose=True
):
    response = glue_client.get_crawler(Name=crawler_name)

    data_sources = response["Crawler"]["Targets"]["S3Targets"]
    for path in data_source_path_list:
        if path not in data_sources:
            data_sources.append({"Path": path})

    response = glue_client.update_crawler(Name=crawler_name, Targets={"S3Targets": data_sources})
    if verbose:
        print("Data sources added successfully")
    if return_response:
        response = get_crawler_if_exists(crawler_name, verbose=False)
        return response


def wait_until_crawler_finish(glue_client, crawler_name, waiting_time=10):
    try:
        previous_state = None
        wait_to_finish = True

        while wait_to_finish:
            response = glue_client.get_crawler(Name=crawler_name)
            crawler_state = response["Crawler"]["State"]

            if crawler_state == "READY":
                print(f"Crawler {crawler_name} is ready")
                print(f"Crawler {crawler_name} finished the run")
                break

            elif crawler_state == "RUNNING":
                if previous_state != "RUNNING":
                    print(f"Crawler {crawler_name} is running")
                previous_state = "RUNNING"

            elif crawler_state == "STOPPING":
                if previous_state != "STOPPING":
                    print(f"Crawler {crawler_name} is stopping")
                previous_state = "STOPPING"

            time.sleep(waiting_time)

        return response

    except Exception as e:
        print(e)


############################### Job code ###############################

stock_prices_file_path = path.join(folder_path_in_s3, f"original_files", f"stock_prices.csv")
average_daily_return_folder_path_in_s3 = path.join(folder_path_in_s3, f"datasets", "average_daily_return/")
avg_frequently_folder_path_in_s3 = path.join(folder_path_in_s3, f"datasets", "avg_frequently/")
volatility_annualized_std_folder_path_in_s3 = path.join(folder_path_in_s3, f"datasets", "volatility_annualized_std/")
stock_prices_30_days_of_return_folder_path_in_s3 = path.join(
    folder_path_in_s3, f"datasets", "stock_prices_30_days_of_return/"
)

# Create Crawler
# folder_path_list = [average_daily_return_folder_path_in_s3, avg_frequently_folder_path_in_s3, volatility_annualized_std_folder_path_in_s3, stock_prices_30_days_of_return_folder_path_in_s3]
# crawler = create_new_crawler(crawler_name, Iam_role,glue_catalog_db_name, folder_path_list)

stock_prices_df = spark.read.csv(stock_prices_file_path, header=True, inferSchema=True)
stock_prices_df = organize_df(stock_prices_df)
stock_prices_with_all_values_df = fill_null_close_values(stock_prices_df)

# Calculate the return_rate
stock_prices_with_all_values_df = calculate_return_rate(
    stock_prices_with_all_values_df, "final_close", "prev_close", "return_rate"
)

# Calculate the average return rate for each day
average_daily_return_df = (
    stock_prices_with_all_values_df.groupBy("date").agg(avg("return_rate").alias("average_return")).orderBy("date")
)

print("Save the average daily return of all stocks for every date partition by year to S3")
average_daily_return_df = average_daily_return_df.withColumn("year", year("date"))
average_daily_return_df.write.partitionBy("year").mode("overwrite").parquet(average_daily_return_folder_path_in_s3)

print(
    "To see the whole table, you can read the file or use athena to query the table, the table name is: average_daily_return"
)
print("The average daily return of all stocks for every date: ")
average_daily_return_df.select("date", "average_return").orderBy(desc("date")).show()

# Calculate the average trading frequency for each stock
stock_prices_with_all_values_df = calculate_frequently(stock_prices_with_all_values_df)
avg_frequently_df = stock_prices_with_all_values_df.groupBy("ticker").agg(avg("frequently").alias("avg_frequently"))
avg_frequently_df = avg_frequently_df.select("ticker", col("avg_frequently").alias("frequently")).orderBy(
    desc("avg_frequently")
)
print("Save the average trading frequency for each stock, partition by the first letter of the stock name to S3")
avg_frequently_df = avg_frequently_df.withColumn("ticker_partition", substring("ticker", 1, 1))
avg_frequently_df.write.partitionBy("ticker_partition").mode("overwrite").parquet(avg_frequently_folder_path_in_s3)
print("The most frequently traded stock: ")
avg_frequently_df.select("ticker", "frequently").orderBy(desc("frequently")).show(1)


# Calculate the most volatile as measured by the annualized standard deviation of daily returns
volatility_annualized_std = calculate_volatile_by_annualized_std(stock_prices_with_all_values_df)
print(
    "Save the volatile stocks by the annualized standard deviation of daily returns, partition by the first letter of the stock name to S3"
)
volatility_annualized_std = volatility_annualized_std.withColumn(
    "ticker_first_letter_partition", substring("ticker", 1, 1)
)
volatility_annualized_std.write.partitionBy("ticker_first_letter_partition").mode("overwrite").parquet(
    volatility_annualized_std_folder_path_in_s3
)

print("The most volatile stock by the annualized standard deviation of daily returns: ")
volatility_annualized_std.select("ticker", "standard deviation", "annualized_std").show(1)

stock_prices_30_days_of_return = calculate_x_days_return_value(stock_prices_with_all_values_df, days_of_return=30)
print("Save the stocks increase in percent of return rate in 30 days, partition by year, month and day to S3")
stock_prices_30_days_of_return = stock_prices_30_days_of_return.withColumn("year", year("date"))
stock_prices_30_days_of_return = stock_prices_30_days_of_return.withColumn("month", month("date"))
stock_prices_30_days_of_return = stock_prices_30_days_of_return.withColumn("day", dayofmonth("date"))
stock_prices_30_days_of_return.write.partitionBy("year", "month", "day").mode("overwrite").parquet(
    stock_prices_30_days_of_return_folder_path_in_s3
)

print("The 3 stocks with the highest increase in percent of return rate in 30 days: ")
stock_prices_30_days_of_return.select("ticker", "date").orderBy(desc(f"return_rate_30_days")).show(3)

# Start the crawler
crawler = get_crawler_if_exists(crawler_name)
start_crawler(crawler_name, crawler)
if wait_to_crawler:
    wait_until_crawler_finish(glue_client, crawler_name)
    print("The Glue catalog is ready to use, you can start to query the tables in Athena")
else:
    print(f"Please wait until the crawler: {crawler_name} finished then you can run query on Athena")
print("Tables names: average_daily_return, avg_frequently, volatility_annualized_std, stock_prices_30_days_of_return")

############################### Job code ###############################

job.commit()
