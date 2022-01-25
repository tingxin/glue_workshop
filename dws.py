from multiprocessing import connection
import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as fn
from secret import get_redshift_secret

redshift_info = get_redshift_secret()

redshift_info = get_redshift_secret()
redshift_host = redshift_info['host']
redshift_port = redshift_info['port']
redshift_jdbc = f"jdbc:redshift://{redshift_host}:{redshift_port}/dev"
redshift_user = redshift_info['username']
redshift_pass = redshift_info['password']

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

read_redshift_options = {
    "url": redshift_jdbc,
    "dbtable": "order_dwd",
    "user": redshift_user,
    "password": redshift_pass,
    "redshiftTmpDir": "s3://tx-glue-workshop/redshift_read_dwd/"
}

# Script generated for node redshift_dwd
redshift_dwd = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options=read_redshift_options,
    additional_options={
        "aws_iam_role": "arn:aws:iam::515491257789:role/AWSGlueServiceRoleDefault"
    },
    transformation_ctx = "redshift_dwd"
)
# use spark api
df = redshift_dwd.toDF()

df = df.groupBy(fn.col('status'),fn.col('city'),fn.col('create_date')).agg(
    fn.count('order_id').alias('order_count'),
    fn.sum('good_count').alias('good_count'),
    fn.sum('amount').alias('amount')
    )

df = df.select("status", "city", "order_count", "good_count", "amount", "create_date")
df.show(1)
# Script generated for node redshift_dws

dyn_df = DynamicFrame.fromDF(df, glueContext, "nested")

wirete_redshift_options = {
    "url": redshift_jdbc,
    "dbtable": "order_dws",
    "user": redshift_user,
    "password": redshift_pass,
    "redshiftTmpDir": "s3://tx-glue-workshop/redshift_dws/"
}

glueContext.write_dynamic_frame.from_options(
    frame=dyn_df,
    connection_type="redshift",
    connection_options=wirete_redshift_options
)

job.commit()

