import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as fn
from datetime import datetime, date, timedelta
import time
from secret import get_secret


# TODO 修改成您的 secret_name， region_name
region_name = "cn-northwest-1"

mysql_secret_name = 'dev/demo/mysql'
redshift_secret_name = 'dev/mall/redshift'

mysql_info = get_secret(mysql_secret_name, region_name)
mysql_jdbc = f"jdbc:mysql://{mysql_info['host']}:{mysql_info['port']}/demo"
mysql_user = mysql_info['username']
mysql_pass = mysql_info['password']

redshift_info = get_secret(redshift_secret_name, region_name)
redshift_jdbc = f"jdbc:redshift://{redshift_info['host']}:{redshift_info['port']}/dev"
redshift_user = redshift_info['username']
redshift_pass = redshift_info['password']

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'mysqlJdbcS3path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 导入模块
begin = datetime(year=2022, month=3, day=12)
diff = timedelta(days=1)
now = datetime.now()
today_begin = datetime(year=now.year, month=now.month, day=now.day)

# TODO 将 s3://tx-glue-workshop/redshift_dwd/ 替换成您的路径
wirete_redshift_options = {
    "url": redshift_jdbc,
    "dbtable": "order_dwd_ex",
    "user": redshift_user,
    "password": redshift_pass,
    "redshiftTmpDir": "s3://txt-glue-code/mall/redsift_temp/"
}

while begin < today_begin:
    next_date = begin + diff
    if next_date > today_begin:
        next_date = today_begin

    expression = f"(select * from order_ex where create_time >= '{begin}' and create_time < '{next_date}') as tview"
    # connection_mysql8_options["dbtable"] = expression

    df_catalog = glueContext.read.format(
        "jdbc").option(
        "url", mysql_jdbc).option(
        "user", mysql_user).option(
        "password", mysql_pass).option(
        "dbtable", expression).option(
        "customJdbcDriverClassName", "com.mysql.cj.jdbc.Driver").option(
        "customJdbcDriverS3Path", args['mysqlJdbcS3path']
    ).load()

    print("got expression ========> {0}".format(expression))
    print("got data ========> {0}".format(df_catalog.count()))
    print("got data ========> {0}".format(df_catalog.show(1)))
    df = df_catalog
    df = df.withColumn("create_date", fn.to_date(df["create_time"]))

    df = df.filter(df["amount"] >= 10)
    dyn_df = DynamicFrame.fromDF(df, glueContext, "nested")

    # example: 写入redshift
    glueContext.write_dynamic_frame.from_options(
        frame=dyn_df,
        connection_type="redshift",
        connection_options=wirete_redshift_options
    )
    print(
        f"success ====>  write {df.count()} records from {begin} to {next_date}")
    begin = next_date
    time.sleep(2)
job.commit()
