from secret import get_redshift_secret
import json

secret_name = "dev/mall/redshift"
region_name = "cn-northwest-1"
redshift_info = get_redshift_secret(secret_name, region_name)
redshift_host = redshift_info['host']
redshift_port = redshift_info['port']
redshift_jdbc = f"jdbc:redshift://{redshift_host}:{redshift_port}/dev"
print(redshift_info)
