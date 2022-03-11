from secret import get_redshift_secret
import json

redshift_info = get_redshift_secret()
redshift_info = get_redshift_secret()
redshift_host = redshift_info['host']
redshift_port = redshift_info['port']
redshift_jdbc = f"jdbc:redshift://{redshift_host}:{redshift_port}/dev"
print(redshift_jdbc)
