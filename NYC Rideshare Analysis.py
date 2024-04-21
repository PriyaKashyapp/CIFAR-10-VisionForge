import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import desc, asc
from datetime import datetime
from pyspark.sql.functions import month,concat_ws
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import unix_timestamp, to_timestamp
from pyspark.sql.functions import to_date, count, col,row_number
from pyspark.sql.window import Window


from graphframes import *

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #Task 1 (15 points): Merging Datasets
    
    #TASK 1(1)- Load CSV File 
    
    #Read Files as rideshare_data CSV file
    rideshare_data = spark.read.options(header='True',inferSchema='True',delimiter=',').csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    
    #Read Files as taxi_zone_lookup CSV file
    taxi_zone_lookup = spark.read.options(header='True',inferSchema='True',delimiter=',').csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")

    rideshare_data.show()
    
#output 
# ---+--------------+----------------+----------------+-----------+----------------+
# |business|pickup_location|dropoff_location|trip_length|request_to_pickup|total_ride_time|on_scene_to_pickup|on_scene_to_dropoff|time_of_day|      date|passenger_fare|driver_total_pay|rideshare_profit|hourly_rate|dollars_per_mile|
# +--------+---------------+----------------+-----------+-----------------+---------------+------------------+-------------------+-----------+----------+--------------+----------------+----------------+-----------+----------------+
# |    Uber|            151|             244|       4.98|            226.0|          761.0|              19.0|              780.0|    morning|1684713600|         22.82|           13.69|            9.13|      63.18|            2.75|
# |    Uber|            244|              78|       4.35|            197.0|         1423.0|             120.0|             1543.0|    morning|1684713600|         24.27|            19.1|            5.17|      44.56|            4.39|
# |    Uber|            151|             138|       8.82|            171.0|         1527.0|              12.0|             1539.0|    morning|1684713600|         47.67|           25.94|           21.73|      60.68|            2.94|
# |    Uber|            138|             151|       8.72|            260.0|         1761.0|              44.0|             1805.0|    morning|1684713600|         45.67|           28.01|           17.66|      55.86|            3.21|
# |    Uber|             36|             129|       5.05|            208.0|         1762.0|              37.0|             1799.0|    morning|1684713600|         33.49|           26.47|            7.02|      52.97|            5.24|
# |    Uber|            138|              88|      12.64|            230.0|         2504.0|              29.0|             2533.0|    morning|1684713600|         69.15|           40.15|            29.0|      57.06|            3.18|
# |    Uber|            200|             138|       14.3|            337.0|         1871.0|             120.0|             1991.0|    morning|1684713600|         62.35|           37.48|           24.87|      67.77|            2.62|
# |    Uber|            182|             242|       1.05|            177.0|          323.0|              30.0|              353.0|    morning|1684713600|          10.3|            6.54|            4.76|       66.7|            6.23|
# |    Uber|            248|             242|       0.57|            195.0|          169.0|               2.0|              171.0|    morning|1684713600|           8.1|            5.54|            2.56|     116.63|            9.72|
# |    Uber|            242|              20|       2.08|            308.0|          822.0|             120.0|              942.0|    morning|1684713600|          14.8|           10.61|            4.19|      40.55|             5.1|
# |    Uber|             20|              20|       0.84|            146.0|          615.0|             121.0|              736.0|    morning|1684713600|         11.11|            7.18|            3.93|      35.12|            8.55|
# |    Uber|            107|             223|      11.79|             84.0|         1779.0|               5.0|             1784.0|    morning|1684713600|         63.37|           48.46|           14.91|      97.79|            4.11|
# |    Uber|            236|             262|       0.91|            176.0|          413.0|              12.0|              425.0|    morning|1684713600|         12.73|             6.4|            7.33|      54.21|            7.03|
# |    Uber|            262|             170|       3.37|            230.0|         1487.0|             132.0|             1619.0|    morning|1684713600|         31.51|            18.4|           13.11|      40.91|            5.46|
# |    Uber|             41|             239|       2.77|            145.0|          801.0|              27.0|              828.0|    morning|1684713600|         40.15|           19.32|           20.83|       84.0|            6.97|
# |    Uber|            239|             239|       1.15|            214.0|          552.0|              15.0|              567.0|    morning|1684713600|         17.14|             6.7|           10.44|      42.54|            5.83|
# |    Uber|             48|             138|      11.98|            229.0|         2455.0|             120.0|             2575.0|    morning|1684713600|         78.88|           49.34|           40.06|      68.98|            4.12|
# |    Uber|            138|             100|       8.54|            241.0|         2517.0|               0.0|             2517.0|    morning|1684713600|         54.74|           34.88|           19.86|      49.89|            4.08|
# |    Uber|            243|              20|       3.16|            180.0|         1405.0|             121.0|             1526.0|    morning|1684713600|         24.83|           17.49|            7.34|      41.26|            5.53|
# |    Uber|             94|              69|       3.58|            354.0|          924.0|             121.0|             1045.0|    morning|1684713600|         21.71|           13.38|            8.33|      46.09|            3.74|
# +--------+---------------+----------------+-----------+-----------------+---------------+------------------+-------------------+-----------+----------+--------------+----------------+----------------+-----------+----------------+
# only showing top 20 rows
    
    taxi_zone_lookup.show()

    
# Output
# +----------+-------------+--------------------+------------+
# |LocationID|      Borough|                Zone|service_zone|
# +----------+-------------+--------------------+------------+
# |         1|          EWR|      Newark Airport|         EWR|
# |         2|       Queens|         Jamaica Bay|   Boro Zone|
# |         3|        Bronx|Allerton/Pelham G...|   Boro Zone|
# |         4|    Manhattan|       Alphabet City| Yellow Zone|
# |         5|Staten Island|       Arden Heights|   Boro Zone|
# |         6|Staten Island|Arrochar/Fort Wad...|   Boro Zone|
# |         7|       Queens|             Astoria|   Boro Zone|
# |         8|       Queens|        Astoria Park|   Boro Zone|
# |         9|       Queens|          Auburndale|   Boro Zone|
# |        10|       Queens|        Baisley Park|   Boro Zone|
# |        11|     Brooklyn|          Bath Beach|   Boro Zone|
# |        12|    Manhattan|        Battery Park| Yellow Zone|
# |        13|    Manhattan|   Battery Park City| Yellow Zone|
# |        14|     Brooklyn|           Bay Ridge|   Boro Zone|
# |        15|       Queens|Bay Terrace/Fort ...|   Boro Zone|
# |        16|       Queens|             Bayside|   Boro Zone|
# |        17|     Brooklyn|             Bedford|   Boro Zone|
# |        18|        Bronx|        Bedford Park|   Boro Zone|
# |        19|       Queens|           Bellerose|   Boro Zone|
# |        20|        Bronx|             Belmont|   Boro Zone|
# +----------+-------------+--------------------+------------+
# only showing top 20 rows
    
    # NOTE : header='true'- showing header , inferSchema='True'- infer datatype of columns from the values 

    #TASK 1(2) Perform Joins (Join rideshare_data with taxi_zone_lookup (pickup_location))

    # Join rideshare_data with taxi_zone_lookup (pickup_location )
    joined_df = rideshare_data.join(taxi_zone_lookup, rideshare_data.pickup_location == taxi_zone_lookup.LocationID)

    # Rename columns 'Borough', 'Zone', and 'service_zone' in joined_df
    joined_df = joined_df.withColumnRenamed("Borough", "Pickup_Borough") \
                         .withColumnRenamed("Zone", "Pickup_Zone") \
                         .withColumnRenamed("service_zone", "Pickup_service_zone")

    # Drop the 'LocationID' column from joined_df
    joined_df = joined_df.drop('LocationID')

    # Join rideshare_data with taxi_zone_lookup (dropoff_location )
    joined_df = joined_df.join(taxi_zone_lookup, rideshare_data.dropoff_location == taxi_zone_lookup.LocationID)

    # Rename columns 'Borough', 'Zone', and 'service_zone' in joined_df
    joined_df = joined_df.withColumnRenamed("Borough", "Dropoff_Borough") \
                         .withColumnRenamed("Zone", "Dropoff_Zone") \
                         .withColumnRenamed("service_zone", "Dropoff_service_zone")

    # Drop the 'LocationID' column from joined_df
    joined_df = joined_df.drop('LocationID')

    # # Show the resulting DataFrame
    joined_df.printSchema()
    
#Output 

 # |-- business: string (nullable = true)
 # |-- pickup_location: integer (nullable = true)
 # |-- dropoff_location: integer (nullable = true)
 # |-- trip_length: double (nullable = true)
 # |-- request_to_pickup: double (nullable = true)
 # |-- total_ride_time: double (nullable = true)
 # |-- on_scene_to_pickup: double (nullable = true)
 # |-- on_scene_to_dropoff: double (nullable = true)
 # |-- time_of_day: string (nullable = true)
 # |-- date: integer (nullable = true)
 # |-- passenger_fare: double (nullable = true)
 # |-- driver_total_pay: double (nullable = true)
 # |-- rideshare_profit: double (nullable = true)
 # |-- hourly_rate: string (nullable = true)
 # |-- dollars_per_mile: string (nullable = true)
 # |-- Pickup_Borough: string (nullable = true)
 # |-- Pickup_Zone: string (nullable = true)
 # |-- Pickup_service_zone: string (nullable = true)
 # |-- Dropoff_Borough: string (nullable = true)
 # |-- Dropoff_Zone: string (nullable = true)
 # |-- Dropoff_service_zone: string (nullable = true)

    #Task 1(3)  convert the UNIX timestamp to the "yyyy-MM-dd" format
   
    joined_df = joined_df.withColumn('date',from_unixtime(joined_df["date"], "yyyy-MM-dd"))
    # joined_df.show()

# Output: 

# ------+--------------------+-------------------|business|pickup_location|dropoff_location|trip_length|request_to_pickup|total_ride_time|on_scene_to_pickup|on_scene_to_dropoff|time_of_day|      date|passenger_fare|driver_total_pay|rideshare_profit|hourly_rate|dollars_per_mile|Pickup_Borough|         Pickup_Zone|Pickup_service_zone|Dropoff_Borough|        Dropoff_Zone|Dropoff_service_zone|
# +--------+---------------+----------------+-----------+-----------------+---------------+------------------+-------------------+-----------+----------+--------------+----------------+----------------+-----------+----------------+--------------+--------------------+-------------------+---------------+--------------------+--------------------+
# |    Uber|            151|             244|       4.98|            226.0|          761.0|              19.0|              780.0|    morning|2023-05-22|         22.82|           13.69|            9.13|      63.18|            2.75|     Manhattan|    Manhattan Valley|        Yellow Zone|      Manhattan|Washington Height...|           Boro Zone|
# |    Uber|            244|              78|       4.35|            197.0|         1423.0|             120.0|             1543.0|    morning|2023-05-22|         24.27|            19.1|            5.17|      44.56|            4.39|     Manhattan|Washington Height...|          Boro Zone|          Bronx|        East Tremont|           Boro Zone|
# |    Uber|            151|             138|       8.82|            171.0|         1527.0|              12.0|             1539.0|    morning|2023-05-22|         47.67|           25.94|           21.73|      60.68|            2.94|     Manhattan|    Manhattan Valley|        Yellow Zone|         Queens|   LaGuardia Airport|            Airports|
# |    Uber|            138|             151|       8.72|            260.0|         1761.0|              44.0|             1805.0|    morning|2023-05-22|         45.67|           28.01|           17.66|      55.86|            3.21|        Queens|   LaGuardia Airport|           Airports|      Manhattan|    Manhattan Valley|         Yellow Zone|
# |    Uber|             36|             129|       5.05|            208.0|         1762.0|              37.0|             1799.0|    morning|2023-05-22|         33.49|           26.47|            7.02|      52.97|            5.24|      Brooklyn|      Bushwick North|          Boro Zone|         Queens|     Jackson Heights|           Boro Zone|
# |    Uber|            138|              88|      12.64|            230.0|         2504.0|              29.0|             2533.0|    morning|2023-05-22|         69.15|           40.15|            29.0|      57.06|            3.18|        Queens|   LaGuardia Airport|           Airports|      Manhattan|Financial Distric...|         Yellow Zone|
# |    Uber|            200|             138|       14.3|            337.0|         1871.0|             120.0|             1991.0|    morning|2023-05-22|         62.35|           37.48|           24.87|      67.77|            2.62|         Bronx|Riverdale/North R...|          Boro Zone|         Queens|   LaGuardia Airport|            Airports|
# |    Uber|            182|             242|       1.05|            177.0|          323.0|              30.0|              353.0|    morning|2023-05-22|          10.3|            6.54|            4.76|       66.7|            6.23|         Bronx|         Parkchester|          Boro Zone|          Bronx|Van Nest/Morris Park|           Boro Zone|
# |    Uber|            248|             242|       0.57|            195.0|          169.0|               2.0|              171.0|    morning|2023-05-22|           8.1|            5.54|            2.56|     116.63|            9.72|         Bronx|West Farms/Bronx ...|          Boro Zone|          Bronx|Van Nest/Morris Park|           Boro Zone|
# |    Uber|            242|              20|       2.08|            308.0|          822.0|             120.0|              942.0|    morning|2023-05-22|          14.8|           10.61|            4.19|      40.55|             5.1|         Bronx|Van Nest/Morris Park|          Boro Zone|          Bronx|             Belmont|           Boro Zone|
# |    Uber|             20|              20|       0.84|            146.0|          615.0|             121.0|              736.0|    morning|2023-05-22|         11.11|            7.18|            3.93|      35.12|            8.55|         Bronx|             Belmont|          Boro Zone|          Bronx|             Belmont|           Boro Zone|
# |    Uber|            107|             223|      11.79|             84.0|         1779.0|               5.0|             1784.0|    morning|2023-05-22|         63.37|           48.46|           14.91|      97.79|            4.11|     Manhattan|            Gramercy|        Yellow Zone|         Queens|            Steinway|           Boro Zone|
# |    Uber|            236|             262|       0.91|            176.0|          413.0|              12.0|              425.0|    morning|2023-05-22|         12.73|             6.4|            7.33|      54.21|            7.03|     Manhattan|Upper East Side N...|        Yellow Zone|      Manhattan|      Yorkville East|         Yellow Zone|
# |    Uber|            262|             170|       3.37|            230.0|         1487.0|             132.0|             1619.0|    morning|2023-05-22|         31.51|            18.4|           13.11|      40.91|            5.46|     Manhattan|      Yorkville East|        Yellow Zone|      Manhattan|         Murray Hill|         Yellow Zone|
# |    Uber|             41|             239|       2.77|            145.0|          801.0|              27.0|              828.0|    morning|2023-05-22|         40.15|           19.32|           20.83|       84.0|            6.97|     Manhattan|      Central Harlem|          Boro Zone|      Manhattan|Upper West Side S...|         Yellow Zone|
# |    Uber|            239|             239|       1.15|            214.0|          552.0|              15.0|              567.0|    morning|2023-05-22|         17.14|             6.7|           10.44|      42.54|            5.83|     Manhattan|Upper West Side S...|        Yellow Zone|      Manhattan|Upper West Side S...|         Yellow Zone|
# |    Uber|             48|             138|      11.98|            229.0|         2455.0|             120.0|             2575.0|    morning|2023-05-22|         78.88|           49.34|           40.06|      68.98|            4.12|     Manhattan|        Clinton East|        Yellow Zone|         Queens|   LaGuardia Airport|            Airports|
# |    Uber|            138|             100|       8.54|            241.0|         2517.0|               0.0|             2517.0|    morning|2023-05-22|         54.74|           34.88|           19.86|      49.89|            4.08|        Queens|   LaGuardia Airport|           Airports|      Manhattan|    Garment District|         Yellow Zone|
# |    Uber|            243|              20|       3.16|            180.0|         1405.0|             121.0|             1526.0|    morning|2023-05-22|         24.83|           17.49|            7.34|      41.26|            5.53|     Manhattan|Washington Height...|          Boro Zone|          Bronx|             Belmont|           Boro Zone|
# |    Uber|             94|              69|       3.58|            354.0|          924.0|             121.0|             1045.0|    morning|2023-05-22|         21.71|           13.38|            8.33|      46.09|            3.74|         Bronx|       Fordham South|          Boro Zone|          Bronx|East Concourse/Co...|           Boro Zone|
# +--------+---------------+----------------+-----------+-----------------+---------------+------------------+-------------------+-----------+----------+--------------+----------------+----------------+-----------+----------------+--------------+--------------------+-------------------+---------------+--------------------+--------------------+
# only showing top 20 rows
    
  
    #Task 1 (4) count(give total numbers of rows)
    
    ## Calculate and print the total number of rows in joined_df
    row_count = joined_df.count()
    print('------------------------------',row_count,'---------------------------------')
    #------------------------------ 69725864 ---------------------------------

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #Task 2 (20 points): Aggregation of Data

    #Task 2(1): Count the number of trips for each business in each month and Draw a Histogram

    # construct a new dataframe named as temp_df , group by ("business") and generate total number of counts per business
    temp_df = joined_df.groupBy("business").count()
    temp_df.show()

# |business|count   |
# +--------+--------+
# |Lyft    |39486   |
# |Uber    |69686378|
# +--------+--------+

    #Extract the month from the 'date' column to group data by business and month.
    current_df = joined_df.withColumn('month', month(joined_df['date']))

    # Combine 'business' and 'month' into a single column, also keeping the 'month' column
    trip_df=current_df.select(concat_ws(' - ',current_df.business,current_df.month).alias('business'),'month')

    # Group data by 'business' and 'month', then count the number of trips in each group.
    trip_df = trip_df.groupBy( "business", "month").count()

    # Display the DataFrame using show()
    trip_df.show()
    
# +--------+-----+--------+
# |business|month|   count|
# +--------+-----+--------+
# |Uber - 3|    3|14554308|
# |Uber - 1|    1|13579077|
# |Lyft - 2|    2|    6491|
# |Uber - 5|    5|14276372|
# |Uber - 2|    2|13280761|
# |Lyft - 5|    5|    9491|
# |Lyft - 1|    1|    6887|
# |Lyft - 4|    4|    8173|
# |Uber - 4|    4|13995860|
# |Lyft - 3|    3|    8444|
# +--------+-----+--------+

    #aggregated data by 'month'
    trip_df.orderBy('month').show()
    

# +--------+-----+--------+
# |business|month|   count|
# +--------+-----+--------+
# |Uber - 1|    1|13579077|
# |Lyft - 1|    1|    6887|
# |Uber - 2|    2|13280761|
# |Lyft - 2|    2|    6491|
# |Lyft - 3|    3|    8444|
# |Uber - 3|    3|14554308|
# |Lyft - 4|    4|    8173|
# |Uber - 4|    4|13995860|
# |Uber - 5|    5|14276372|
# |Lyft - 5|    5|    9491|
# +--------+-----+--------+

    
    # writing trip_df dataframe to CSV file named as businessMtrips.csv, consolidating into one file for ease of access using coalesce() and write().
    trip_df.coalesce(1).write.option("header",True).option("delimiter",",").csv("s3a://" + s3_bucket + "/businessMtrips.csv")
   
    
    #Task 2(2): Calculate the monthly profit for each business

    # Convert 'rideshare_profit' to float
    monthlyprofit_df = current_df.withColumn("rideshare_profit", current_df["rideshare_profit"].cast("float"))

    # Aggregate 'rideshare_profit' by 'business' and 'month', summing up profits.
    monthlyprofit_df = monthlyprofit_df.groupBy( "business", "month").sum('rideshare_profit')

    # Rename aggregation "sum(rideshare_profit)" to "rideshare_profit" as asked in question.
    monthlyprofit_df = monthlyprofit_df.withColumnRenamed("sum(rideshare_profit)", "rideshare_profit")

    # concatinate 'business' column with 'month' column, while retaining 'rideshare_profit'.
    monthlyprofit_df = monthlyprofit_df.select(concat_ws(' - ',monthlyprofit_df.business,monthlyprofit_df.month).alias('business'),'rideshare_profit')

    #write the DataFrame to CSV named as platformprofit.csv, consolidating into one file for ease of access using coalesce() and write().
    monthlyprofit_df.coalesce(1).write.option("header",True).option("delimiter",",").csv("s3a://" + s3_bucket + "/platformprofit.csv")

    # Display the DataFrame using show()
    monthlyprofit_df.show()

# Output:

# +--------+--------------------+
# |business|    rideshare_profit|
# +--------+--------------------+
# |Lyft - 4|  -90197.13001759537|
# |Uber - 5|1.6313361550055727E8|
# |Uber - 4|1.5026982019417086E8|
# |Lyft - 3|  -99403.93998675235|
# |Uber - 2|1.3062880563633615E8|
# |Lyft - 1|   -72633.3500049822|
# |Uber - 3|1.5207287641912195E8|
# |Uber - 1|1.3319711162465689E8|
# |Lyft - 2|  -70064.72000297531|
# |Lyft - 5| -107719.21000343934|
# +--------+--------------------+

    #Task 2(3): Calculate the driver's earnings for each business in each month


    # Convert 'driver_total_pay' to float for precise calculations using cast().
    driverearnings_df = current_df.withColumn("driver_total_pay", current_df["driver_total_pay"].cast("float"))

    # Aggregate total pay by 'business' and 'month', summing up with sum().
    driverearnings_df = driverearnings_df.groupBy("business", "month").sum('driver_total_pay')

    # Rename aggregated column using withColumnRenamed().
    driverearnings_df = driverearnings_df.withColumnRenamed("sum(driver_total_pay)", "driver_total_pay")

    # Merge 'business' and 'month' into a single column for detailed identification using concat_ws().
    driverearnings_df = driverearnings_df.select(concat_ws(' - ', driverearnings_df.business, driverearnings_df.month).alias('business'), 'driver_total_pay')

    # Save the DataFrame to a CSV file and named as driverearning.csv,consolidating into one file for ease of access using coalesce() and write().
    driverearnings_df.coalesce(1).write.option("header", True).option("delimiter", ",").csv("s3a://" + s3_bucket + "/driverearning.csv")

    # Display the processed DataFrame using show().
    driverearnings_df.show()

#Output : 

# +--------+---------------------+
# |business|     driver_total_pay|
# +--------+---------------------+
# |Lyft-1  |    239932.2593984604|
# |Uber-1  | 2.5025348066162884E8|
# |Uber-2  |  2.521559770842399E8|
# |Lyft-2  |    234875.5296087265|
# |Lyft-3  |   310276.54944992065|
# |Uber-3  |    2.9595849599784E8|
# |Lyft-4  |    297815.3794941902|
# |Uber-4  |  2.950689272036143E8|
# |Uber-5  |  3.130051145308317E8|
# |Lyft-5  |   360408.08971881866|
# +--------+---------------------+

    # Task 2(4): Extracting Insights and Making Decisions
    # Explaination provided in PDF file. 

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    #Task 3(1): Identify the top 5 popular pickup boroughs each month


    # Aggregate trips by Pickup Borough and month, counting the number of trips in each category.
    Tripcount_df = current_df.groupBy( "Pickup_Borough", "month").count()

    # Rename the count column to 'trip_count'
    Tripcount_df = Tripcount_df.withColumnRenamed("count", "trip_count")

    #window specification to partition data by month and order by trip_count in descending order.
    windowDept = Window.partitionBy("month").orderBy(col("trip_count").desc())

    # print top 3 rows, rank each trip month wise using window specification
    Tripcount_df=Tripcount_df.withColumn("row",row_number().over(windowDept)).filter(col("row") <= 5)\
                             .drop("row")

    #DataFrame by month to display the top 5 popular pickup boroughs
    Tripcount_df.orderBy('month').show()

# output:
# +--------------+-----+----------+
# |Pickup_Borough|month|trip_count|
# +--------------+-----+----------+
# |     Manhattan|    1|   5854818|
# |      Brooklyn|    1|   3360373|
# |        Queens|    1|   2589034|
# |         Bronx|    1|   1607789|
# | Staten Island|    1|    173354|
# |        Queens|    2|   2447213|
# |         Bronx|    2|   1581889|
# |     Manhattan|    2|   5808244|
# |      Brooklyn|    2|   3283003|
# | Staten Island|    2|    166328|
# |      Brooklyn|    3|   3632776|
# |         Bronx|    3|   1785166|
# |     Manhattan|    3|   6194298|
# |        Queens|    3|   2757895|
# | Staten Island|    3|    191935|
# |     Manhattan|    4|   6002714|
# |        Queens|    4|   2666671|
# |         Bronx|    4|   1677435|
# |      Brooklyn|    4|   3481220|
# | Staten Island|    4|    175356|
# +--------------+-----+----------+

    #Task 3(2): Identify the top 5 popular dropoff boroughs each month


    # print number of trips by aggregate tris by Dropoff Borough and month
    Tripcount_df = current_df.groupBy( "Dropoff_Borough", "month").count()

    # Rename the count column as 'trip_count'.
    Tripcount_df = Tripcount_df.withColumnRenamed("count", "trip_count")

    # used window specification to partition data by month, ordered by descending trip counts
    windowDept = Window.partitionBy("month").orderBy(col("trip_count").desc())

    # Apply the window spec to rank dropoff locations within each month, retaining only the top 5.
    Tripcount_df=Tripcount_df.withColumn("row",row_number().over(windowDept)).filter(col("row") <= 5)\
                             .drop("row")

    # Sort and display the data by month to visualize the top 5 dropoff boroughs chronologically.
    Tripcount_df.orderBy('month').show()


# Outpt
# +---------------+-----+----------+
# |Dropoff_Borough|month|trip_count|
# +---------------+-----+----------+
# |       Brooklyn|    1|   3337415|
# |          Bronx|    1|   1525137|
# |      Manhattan|    1|   5444345|
# |         Queens|    1|   2480080|
# |        Unknown|    1|    535610|
# |        Unknown|    2|    497525|
# |      Manhattan|    2|   5381696|
# |       Brooklyn|    2|   3251795|
# |         Queens|    2|   2390783|
# |          Bronx|    2|   1511014|
# |      Manhattan|    3|   5671301|
# |        Unknown|    3|    566798|
# |       Brooklyn|    3|   3608960|
# |         Queens|    3|   2713748|
# |          Bronx|    3|   1706802|
# |      Manhattan|    4|   5530417|
# |       Brooklyn|    4|   3448225|
# |         Queens|    4|   2605086|
# |          Bronx|    4|   1596505|
# |        Unknown|    4|    551857|
# +---------------+-----+----------+


    #Task 3(3): Identify the top 30 earnest routes


    # Group data by Pickup and Dropoff Boroughs, summing up 'driver_total_pay' for each route.
    route_df = current_df.groupBy("Pickup_Borough","Dropoff_Borough").sum("driver_total_pay")

    # renamed "sum(driver_total_pay)" to "total_profit" using withColumnRenamed
    route_df = route_df.withColumnRenamed("sum(driver_total_pay)", "total_profit")

    # Create a 'Route' column by concatenating Pickup and Dropoff Boroughs, delineated by ' to '.
    route_df=route_df.select(concat_ws(' to ',route_df.Pickup_Borough,route_df.Dropoff_Borough,)
           .alias("Route"),"total_profit")

    # Sort the routes by 'total_profit' in descending order and display the top 30 most profitable routes.
    route_df.orderBy("total_profit", asc = False).show(30)


# utput 
# +--------------------+--------------------+
# |               Route|        total_profit|
# +--------------------+--------------------+
# |    EWR to Manhattan|                62.0|
# |Unknown to Staten...|              259.02|
# |      Unknown to EWR|              431.16|
# |          EWR to EWR|  458.06999999999994|
# | Unknown to Brooklyn|   5356.040000000001|
# |    Unknown to Bronx|   8169.179999999998|
# |Unknown to Manhattan|  10024.059999999998|
# |  Unknown to Unknown|  18253.869999999995|
# |   Unknown to Queens|  19936.049999999996|
# |Staten Island to ...|  203073.06000000003|
# |Bronx to Staten I...|           207356.61|
# |        Bronx to EWR|           381787.08|
# |Staten Island to EWR|           547553.22|
# |Staten Island to ...|   807064.1199999999|
# |Queens to Staten ...|   865603.3800000004|
# |Staten Island to ...|           891285.81|
# |       Queens to EWR|  1192758.6600000001|
# |Staten Island to ...|  1612227.7200000004|
# |Manhattan to Stat...|          2223727.37|
# |Staten Island to ...|  2265856.4600000004|
# |Brooklyn to State...|  2417853.8200000003|
# |     Brooklyn to EWR|  3292761.7100000014|
# |   Bronx to Brooklyn|          5629874.41|
# |   Brooklyn to Bronx|   5848822.560000001|
# |Staten Island to ...|   9686862.450000014|
# |     Queens to Bronx|1.0182898729999997E7|
# |     Bronx to Queens|1.0292266499999993E7|
# |    Bronx to Unknown|1.0464800209999995E7|
# | Brooklyn to Unknown|1.0848827569999997E7|
# |    Manhattan to EWR|2.3750888619999994E7|
# +--------------------+--------------------+
    

    # Task 3(4) Stakeholder Insights and Decisions
    # Explaination provided in PDF file. 

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    
    #Task 4(1): Calculate the average 'driver_total_pay' during different 'time_of_day' periods
    
    # Group data by time of day and calculate the average driver total pay for each period.
    avgpay_df = current_df.groupBy("time_of_day").avg("driver_total_pay")

    # Rename the resulting average column for clarity to 'average_drive_total_pay'.
    avgpay_df = avgpay_df.withColumnRenamed("avg(driver_total_pay)", "average_drive_total_pay")

    # Sort the data by average driver total pay in descending order to identify which time of day has the highest earnings.
    avgpay_df.orderBy("average_drive_total_pay", desc = True).show()



# Output
# +-----------+-----------------------+
# |time_of_day|average_drive_total_pay|
# +-----------+-----------------------+
# |    morning|     19.633332793944835|
# |    evening|     19.777427702398388|
# |      night|      20.08743800359271|
# |  afternoon|     21.212428756593535|
# +-----------+-----------------------+

    #Task 4(2): Calculate the average 'trip_length' during different 'time_of_day' periods

    # used groupBy to calculate the average "trip_length" for each "time_of_day"
    avgtrip_df = current_df.groupBy("time_of_day").avg("trip_length")

    # Rename the "avg(trip_length)" as "average_trip_length".
    avgtrip_df = avgtrip_df.withColumnRenamed("avg(trip_length)", "average_trip_length")

    # Order the results by average trip length in descending order.
    avgtrip_df.orderBy("average_trip_length", ascending = False).show()


# Output
# +-----------+-------------------+
# |time_of_day|average_trip_length|
# +-----------+-------------------+
# |      night|   5.32398480196174|
# |    morning|  4.927371866442786|
# |  afternoon|  4.861410525661208|
# |    evening|  4.484750367447518|
# +-----------+-------------------+

    

    #Task 4(3): Calculate the average earned per mile for each 'time_of_day' period

    # perform join on "avgpay_df" and "avgpay_df" dataframe on the 'time_of_day'
    avgearnedpermile_df = avgpay_df.join(avgtrip_df, avgpay_df.time_of_day == avgtrip_df.time_of_day)

    # Calculate the average earning per mile by dividing the average trip length by the average driver total pay.
    avgearnedpermile_df = avgearnedpermile_df.select((avgtrip_df.average_trip_length/        avgpay_df.average_drive_total_pay).alias('average_earning_per_mile'),("time_of_day"))

    # Display the calculated average earning per mile for each time of day.
    avgearnedpermile_df.show()


# Output
# +-----------+-------------------+
# |time_of_day|average_trip_length|
# +-----------+-------------------+
# |  afternoon|  4.363430846355172|
# |      night| 3.7730083773652163|
# |    morning| 3.9845445586023174|
# |    evening|  4.409928419407853|
# +-----------+-------------------+   


    #Task 4(4): Insights and Decisions
    #Solution : Explaination provided in PDF file. 
    
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    # Task 5 (15 points): Finding anomalies


    #Task 5(1): Calculate Average Waiting Time in January

    # .filter() used to filter data for month of Janurary
    Jan_df = current_df.filter(current_df.month==1)

    # Select relevant columns for waiting time analysis: 'month', 'request_to_pickup', and 'date'.
    Jan_df = Jan_df.select('month','request_to_pickup','date')

    # Group by 'date' to calculate the average waiting time ('request_to_pickup') for each day in January. 
    Jan_df = Jan_df.groupby('date').avg('request_to_pickup')

    # renamed the column from "avg(request_to_pickup)" to "request_to_pickup"
    Jan_df = Jan_df.withColumnRenamed("avg(request_to_pickup)", "request_to_pickup")

     # Save the DataFrame to a CSV file and named as averagewaitingtime.csv,consolidating into one file for ease of access using coalesce() and write().
    Jan_df.coalesce(1).write.option("header",True).option("delimiter",",").csv("s3a://" + s3_bucket + "/averagewaitingtime.csv")

    # Display the sorted list of days by average waiting time in descending order
    Jan_df.orderBy('request_to_pickup', ascending = False).show()


#Output :
# +----------+------------------+
# |      date| request_to_pickup|
# +----------+------------------+
# |2023-01-01| 396.5318744409635|
# |2023-01-22|277.49287089443135|
# |2023-01-19|272.02203820618143|
# |2023-01-15| 268.5346481777792|
# |2023-01-21| 266.6804386133228|
# |2023-01-25| 261.2912811176952|
# |2023-01-12|255.17599322195403|
# |2023-01-28| 254.6833639623887|
# |2023-01-29| 254.2460334757214|
# |2023-01-16|251.55102299494047|
# |2023-01-31|248.65506923045416|
# |2023-01-14|247.49345781069232|
# |2023-01-23|247.32448989998323|
# |2023-01-08|246.41358687741243|
# |2023-01-02|246.05148716456986|
# |2023-01-20|243.43761253646377|
# |2023-01-26|242.56764282565965|
# |2023-01-17| 240.5772885527869|
# |2023-01-13|239.22308233638282|
# |2023-01-27|236.93431696586904|
# +----------+------------------+


    #Task 5(2)Which day(s) does the average waiting time exceed 300 seconds?

    #Solution : from above table ist |2023-01-01|


    #Task 5(3)Why was the average waiting time longer on these day(s) compared to other days?

    #Solution : Explaination provided in PDF file. 
    
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    # Task 6 (15 points): Filtering Data


    # Task 6(1) Analyzing Urban Mobility : Trip Count Dynamics by Pickup Borough and Time of Day,

    # Group data by Pickup Borough and time of day, then count the number of trips for each group
    trip_count_df = current_df.groupBy('Pickup_Borough','time_of_day').count()

    # Filter to retain groupings with a trip count of 1000 or less
    trip_count_df = trip_count_df.filter(col('count')<= 1000)

    # Rename the count column to 'trip_count'
    trip_count_df = trip_count_df.withColumnRenamed('count','trip_count')

    # display the resulted Dataframe
    trip_count_df.show()

# Output :
# +--------------+-----------+----------+
# |Pickup_Borough|time_of_day|trip_count|
# +--------------+-----------+----------+
# |           EWR|      night|         3|
# |           EWR|  afternoon|         2|
# |       Unknown|    morning|       892|
# |       Unknown|  afternoon|       908|
# |       Unknown|    evening|       488|
# |           EWR|    morning|         5|
# |       Unknown|      night|       792|
# +--------------+-----------+----------+

    # Task6(2) : Calculate the number of trips for each 'Pickup_Borough' in the evening time.
    
    # Group data by Pickup Borough and time of day, then count the number of trips for each grouping.
    trip_count_df = current_df.groupBy('Pickup_Borough','time_of_day').count()

    # filtyer the trip by "evening" in trip_of_day column
    trip_count_df = trip_count_df.filter(col('time_of_day')== 'evening') 

    # rename the column as "trip_count"
    trip_count_df = trip_count_df.withColumnRenamed('count','trip_count')

    # Display the datatframe
    trip_count_df.show()


# Output :
# +--------------+-----------+----------+
# |Pickup_Borough|time_of_day|trip_count|
# +--------------+-----------+----------+
# |         Bronx|    evening|   1380355|
# |        Queens|    evening|   2223003|
# |     Manhattan|    evening|   5724796|
# | Staten Island|    evening|    151276|
# |      Brooklyn|    evening|   3075616|
# |       Unknown|    evening|       488|
# +--------------+-----------+----------+


    #Task6(3)  Calculate the number of trips that started in Brooklyn (Pickup_Borough field) and ended in Staten Island (Dropoff_Borough field)


    # Aggregate data to count trips by Pickup Borough, Dropoff Borough, and Pickup Zone.
    trip_count_df = current_df.groupBy('Pickup_Borough', 'Dropoff_Borough', 'Pickup_Zone').count()

    # Filter the trips from Brooklyn to Staten Island.
    trip_count_df = trip_count_df.filter((col('Pickup_Borough') == 'Brooklyn') & (col('Dropoff_Borough') == 'Staten Island'))

    # Drop the 'count' column.
    trip_count_df = trip_count_df.drop('count')

    # showing the first 10 entries without.
    trip_count_df.show(10, truncate=False)


# Output:    
# +--------------+---------------+----------------------+
# |Pickup_Borough|Dropoff_Borough|Pickup_Zone           |
# +--------------+---------------+----------------------+
# |Brooklyn      |Staten Island  |Fort Greene           |
# |Brooklyn      |Staten Island  |Bushwick North        |
# |Brooklyn      |Staten Island  |Cobble Hill           |
# |Brooklyn      |Staten Island  |Marine Park/Mill Basin|
# |Brooklyn      |Staten Island  |Bushwick South        |
# |Brooklyn      |Staten Island  |East Williamsburg     |
# |Brooklyn      |Staten Island  |Crown Heights South   |
# |Brooklyn      |Staten Island  |East New York         |
# |Brooklyn      |Staten Island  |Prospect Park         |
# |Brooklyn      |Staten Island  |Windsor Terrace       |
# +--------------+---------------+----------------------+

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


    # Task 7 (15 points): Routes Analysis


    # concatenating 'Pickup_Zone' and 'Dropoff_Zone'
    route_df = joined_df.withColumn('Route', concat(col('Pickup_Zone'), lit(' to '), col('Dropoff_Zone')))

    # Group by 'Route' and 'business', and use count operation
    group_df = route_df.groupBy('Route', 'business').count()

    # DataFrame to get separate columns for Uber and Lyft counts
    pivot_df = group_df.groupBy('Route').pivot('business', ['Uber', 'Lyft']).sum('count')

    # Fill null values with 0
    pivot_df = pivot_df.fillna(0)
    
    # Rename the uber and lyft to uber_count and lyft_count
    pivot_df = pivot_df.withColumnRenamed('Uber','uber_count').withColumnRenamed('Lyft','lyft_count')

    # total count for each route
    pivot_df = pivot_df.withColumn('total_count', pivoted_df['uber_count'] + pivoted_df['lyft_count'])

    # total count in descending order and select the top 10 routes
    totcount_df = pivot_df.orderBy(col('total_count').desc()).limit(10)

    # top 10 popular routes
    totcount_df.show(truncate = False)

#Output :
# +---------------------------------------------+----------+----------+-----------+
# |Route                                        |uber_count|lyft_count|total_count|
# +---------------------------------------------+----------+----------+-----------+
# |JFK Airport to NA|21605473                   |253211    |46        |253257     |
# |East New York to East New York               |202719    |184       |202903     |
# |Borough Park to Borough Park                 |155803    |78        |155881     |
# |LaGuardia Airport to NA                      |151521    |41        |151562     |
# |Canarsie to Canarsie                         |126253    |26        |126279     |
# |South Ozone Park to JFK Airport              |107392    |1770      |109162     |
# |Crown Heights North to Crown Heights North   |98591     |100       |98691      |
# |Bay Ridge to Bay Ridge                       |98274     |300       |98574      |
# |Astoria to Astoria                           |90692     |75        |90767      |
# |Jackson Heights to Jackson Heights           |89652     |19        |89671      |
# +---------------------------------------------+----------+----------+-----------+


    spark.stop()
    
## COMMONDS TO RUN
## oc logs -f filename-spark-app-driver
## ccc create spark filename.py -s