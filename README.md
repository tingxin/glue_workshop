# 使用Glue进行大数据建设
AWS Glue 是用于提取、转换和加载 (ETL) 操作的无服务器数据准备服务。它使数据工程师、数据分析师、数据科学家以及 ETL 开发人员能够轻松地提取、清理、丰富、规范化和加载数据。AWS Glue 将开始分析数据所需的时间从数月缩短到几分钟。它为您提供了直观和基于代码的界面，使数据准备过程变得简单轻松。数据工程师和 ETL 开发人员只需单击几下鼠标，就可以使用 AWS Glue Studio 创建、运行和监控 ETL 作业。数据分析师和数据科学家可以使用 AWS Glue DataBrew 直观地清理和规范化数据，而无需编写代码。

## 代码介绍
1. dwd.py 和 dws.py 具体的数据 ETL 代码
2. mock.py 模拟生成订单数据
3. secret.py 通过secret manager 生成的代码，需要修改注释部分 TODO 部分

## 操作文档
参考 使用Glue进行大数据建设.pdf

## 更多相关workshop资源
```
https://github.com/toreydai/serverless-analytics-workshop
https://github.com/aws-samples/amazon-redshift-commands-using-aws-glue
```