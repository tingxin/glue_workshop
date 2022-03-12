```
# 使用Glue 进行数仓建设
## 准备工作
1. mysql主库终端节点
```

shop.c6lwjjfhbm6a.rds.cn-northwest-1.amazonaws.com.cn
```
2. mysql从库终端节点
```
readonly1.c6lwjjfhbm6a.rds.cn-northwest-1.amazonaws.com.cn
```
3. redshift 集群终端节点
```
redshift-cluster-1.cmnyuhfynqj7.cn-northwest-1.redshift.amazonaws.com.cn:5439/dev

```
4. 创建数据表
```
mysql -h demo.c6lwjjfhbm6a.rds.cn-northwest-1.amazonaws.com.cn -P 3306 -u admin -p

create database weige;

use weige;

CREATE TABLE IF NOT EXISTS `order` (
    order_id INT AUTO_INCREMENT NOT NULL,
    user_mail varchar(20) NOT NULL,
    status char(10) NOT NULL, 
    good_count INT NOT NULL,
    city varchar(20) NOT NULL,
    amount FLOAT NOT NULL,
    create_time datetime NOT NULL,
    update_time datetime NOT NULL,
    PRIMARY KEY (`order_id`)
);
```
5. 插入假数据
```
python3 mock.py
```


## 创建 mysql 链接
1. 使用mysql 从库创建 glue 的数据链接
```
jdbc:mysql://readonly1.c6lwjjfhbm6a.rds.cn-northwest-1.amazonaws.com.cn:3306/weige
```

2. 将 mysql JDBC 驱动提前上传到 S3 指定存储桶中:
```
curl -O https://cdn.mysql.com/archives/mysql-connector-java-
8.0/mysql-connector-java-8.0.26.zip

unzip mysql-connector-java-8.0.26.zip

cd mysql-connector-java-8.0.26/

aws s3 cp mysql-connector-java-8.0.26.jar s3://nwcd-camp-bucket/jdbc/
```
## 配置 secretsmanager

## 构建 dwd job
1. glue studio 创建 dwd作业，选择空白python script
2. 修改作业配置，将依赖库secret.py通过aws cli上传到s3,并配置在作业的依赖库选项中
3. 在作业高级属性中添加mysql 相关参数 
4. 注意配置mysql 链接信息

```
'dbuser', 'dbpassword', 'dburl', 'mysqlJdbcS3path'
 
```

## 如上类似构建dws job

## 构建workflow

## 更多资源
```
https://github.com/toreydai/serverless-analytics-workshop
https://github.com/aws-samples/amazon-redshift-commands-using-aws-glue
```

```