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
begin_id = 0
delta = 50000

# TODO 将 s3://tx-glue-workshop/redshift_dwd/ 替换成您的路径
wirete_redshift_options = {
    "url": redshift_jdbc,
    "dbtable": "order_dwd_ex2",
    "user": redshift_user,
    "password": redshift_pass,
    "redshiftTmpDir": "s3://txt-glue-code/mall/redsift_temp/"
}

# use spark api
# 重点，这里要修改 redshift_expression 为你的SQL
redshift_expression = '(select max(order_id) as max_id from public.order_dwd_ex2 limit 1) as tview'
df_redshift = glueContext.read.format(
    "jdbc").option(
    "url", redshift_jdbc).option(
    "user", redshift_user).option(
    "password", redshift_pass).option(
    "dbtable", redshift_expression).option(
    "aws_iam_role", "arn:aws:iam::515491257789:role/AWSGlueServiceRoleDefault").option(
    "redshiftTmpDir", "s3://txt-glue-code/mall/redshift_temp3/"
).load()

row = df_redshift.collect()[0]
begin_id = row[0]
print(f"will begin from {begin_id}")

while True:
    next_id = begin_id + delta

    expression = f"(select * from order_ex where order_id >= {begin_id} and order_id < {next_id}) as tview"
    # connection_mysql8_options["dbtable"] = expression

    df_catalog = glueContext.read.format(
        "jdbc").option(
        "url", mysql_jdbc).option(
        "user", mysql_user).option(
        "password", mysql_pass).option(
        "dbtable", expression).option(
        "customJdbcDriverClassName", "com.mysql.cj.jdbc.Driver").option(
        "customJdbcDriverS3Path", args['mysqlJdbcS3path']).load()

    print("got expression ========> {0}".format(expression))
    print("got data ========> {0}".format(df_catalog.count()))
    print("got data ========> {0}".format(df_catalog.show(1)))
    data_count = df_catalog.count()

    # 重点
    if data_count == 0:
        break

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
        f"success ====>  write {df.count()} records from {begin_id} to {next_id}")

    begin_id = next_id

    time.sleep(2)


job.commit()
