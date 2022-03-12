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
from secret import get_redshift_secret

# TODO 修改成您的 secret_name， region_name
secret_name = "dev/mall/redshift"
region_name = "cn-northwest-1"

redshift_info = get_redshift_secret(secret_name, region_name)
redshift_host = redshift_info['host']
redshift_port = redshift_info['port']
redshift_jdbc = f"jdbc:redshift://{redshift_host}:{redshift_port}/dev"
redshift_user = redshift_info['username']
redshift_pass = redshift_info['password']

args = getResolvedOptions(
    sys.argv, ['dbuser', 'dbpassword', 'dburl', 'mysqlJdbcS3path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("glue_mysql8", args)

# 从MySQL中读取数据
connection_mysql8_options = {
    "url": args['dburl'],
    "dbtable": "order",
    "user": args['dbuser'],
    "password": args['dbpassword'],
    "customJdbcDriverS3Path": args['mysqlJdbcS3path'],
    "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver",
    "jobBookmarkKeys": ["order_id"],
    "jobBookmarkKeysSortOrder": "asc"
}

# # 如果表中有自增id,或者唯一值数值类递增字段， 可以使用如下方式进行增量同步
connection_mysql8_options['jobBookmarkKeys'] = ['order_id']
connection_mysql8_options['jobBookmarkKeysSortOrder'] = 'asc'

# # 如果表中没有有增id,或者唯一值数值类递增字段， 可以使用如下方式进行增量同步
# today_begin = datetime.now().strftime("%Y-%m-%d 00:00:00")
# connection_mysql8_options['hashexpression'] = "create_time >= '" + today_begin + "' AND create_time"
# connection_mysql8_options['hashpartitions'] = "10"

df_catalog = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options=connection_mysql8_options,
    # 如果要使用书签，这个上下文不能忽略
    transformation_ctx="df_catalog"
)


# use glue api
# df_filter = Filter.apply(frame = df_catalog, f = lambda x: x["amount"] >=10)
# use spark api
df = df_catalog.toDF()
df = df.filter(df["amount"] >= 10)
df = df.withColumn("create_date", fn.to_date(df["create_time"]))
df.show(10)
print("========> {0}".format(df.count()))
dyn_df = DynamicFrame.fromDF(df, glueContext, "nested")

# TODO 将 s3://tx-glue-workshop/redshift_dwd/ 替换成您的路径
wirete_redshift_options = {
    "url": redshift_jdbc,
    "dbtable": "order_dwd",
    "user": redshift_user,
    "password": redshift_pass,
    "redshiftTmpDir": "s3://txt-glue-code/mall/redsift_temp/"
}

# example: 写入redshift
glueContext.write_dynamic_frame.from_options(
    frame=dyn_df,
    connection_type="redshift",
    connection_options=wirete_redshift_options
)

# # example: 通过glue df_catalog 写入redshift

# AmazonGlueDataCatalog_node1647005686769 = glueContext.write_dynamic_frame.from_catalog(
#     frame=dyn_df,
#     database="mall",
#     table_name="dev_public_order_dwd",
#     redshift_tmp_dir="s3://txt-glue-code/mall/redsift_temp/",
#     additional_options={
#         "aws_iam_role": "arn:aws-cn:iam::027040934161:role/AWSGlueDefault"
#     },
#     transformation_ctx="AmazonGlueDataCatalog_node1647005686769",
# )

# # example: 写入s3
# # TODO 将 s3://tx-glue-workshop/s3_dwd/ 替换成您的路径
# glueContext.write_dynamic_frame.from_options(
#     frame=dyn_df,
#     connection_type="s3",
#     format="parquet",
#     connection_options={"path": "s3://tx-glue-workshop/s3_dwd/", "partitionKeys": ["create_date"]},
#     transformation_ctx="S3bucket_node3",
# )

# 写入mysql
# wirete_mysql_options = {
#     "url": mysql_jdbc,
#     "dbtable": "order_dwd",
#     "user": 'admin',
#     "password": 'Demo1234',

# }


# glueContext.write_dynamic_frame.from_options(
#     frame=dyn_df,
#     connection_type="mysql",
#     connection_options=wirete_redshift_options
# )
job.commit()
