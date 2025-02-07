from hdfs import InsecureClient

client = InsecureClient('http://172.20.0.3:50070', user='hadoop')
directories = client.list('/')

print(client.url)
