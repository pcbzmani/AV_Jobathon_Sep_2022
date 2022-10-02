# Import libraries
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# Function input - spark object, click data path, resolved data path
# Function output - final spark dataframe# 
def sample_function(spark, s3_clickstream_path, s3_login_path):
  df_clickstream =  spark.read.format("json").load(s3_clickstream_path)
  user_mapping =  spark.read.format("csv").option("header",True).load(s3_login_path)

# Join clickstream with user mapping  
  df = df_clickstream.join(
    user_mapping,
    'session_id',
    'left_outer'
  ) 

# Derive Current date, Page URL and Logged in
  df = df.withColumn('client_page_url', F.col('client_side_data').getField("current_page_url"))\
       .withColumn('current_date',F.split('event_date_time',' ')[0])\
       .withColumn('login_date',F.split('login_date_time',' ')[0])\
       .drop('client_side_data','login_date_time')

# Derive Click count and Page count for registered users
  df_grp = df.groupBy('current_date','browser_id','user_id')\
           .pivot('event_type').agg({'event_type':'count'})


# Join the event grouped DF with Original df  
  df = df.join(
    df_grp,
    [df['user_id'].eqNullSafe(df_grp['user_id']),df['current_date']==df_grp['current_date'],df['browser_id']==df_grp['browser_id']],
    'left_outer'
  ).drop(df_grp['user_id'])\
   .drop(df_grp['browser_id'])\
   .drop(df_grp['current_date'])
  
#Window Spec
  window_spec = Window\
              .partitionBy('current_date','browser_id','user_id')\
              .orderBy(F.col('event_date_time').asc())
  
  df = df.withColumn('date_diff',F.datediff(F.col('current_date'),F.col('login_date')))\
         .withColumn('logged_in',F.when((F.col('date_diff') == 0 ),F.lit(1))\
                               .otherwise(F.lit(0)))\
        .withColumn('row_number',F.rank().over(window_spec))\
        .withColumn('number_of_pageloads',F.coalesce(F.col('pageload'),F.lit(0) )) \
        .withColumn('number_of_clicks',F.coalesce(F.col('click'),F.lit(0) ))\
        .filter('row_number == 1')\
        .select('current_date','browser_id','user_id','logged_in',F.col('client_page_url').alias('first_url'),\
                'number_of_clicks','number_of_pageloads')) 
              
  return df
