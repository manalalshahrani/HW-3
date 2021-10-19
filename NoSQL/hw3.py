import boto3
import csv

session = boto3.Session(
aws_access_key_id='',
aws_secret_access_key=''
)


s3 = session.resource('s3')


object = s3.Object('hw3-cmu', 'experiments.csv').put( 
       Body = open('experiments.csv','rb'))
object = s3.Object('hw3-cmu', 'exp1.csv').put( 
       Body = open('exp1.csv','rb'))

object = s3.Object('hw3-cmu', 'exp2.csv').put( 
       Body = open('exp2.csv','rb'))

object = s3.Object('hw3-cmu', 'exp3.csv').put( 
       Body = open('exp3.csv','rb'))


dynamodb = boto3.resource('dynamodb')

table = dynamodb.create_table(
    TableName='experiments',
    KeySchema=[
        {
            'AttributeName': 'id', 
            'KeyType': 'HASH'
        }
       
    ], 
    AttributeDefinitions=[
          {
            'AttributeName': 'id', 
            'AttributeType': 'N'
        },

    ], 
    ProvisionedThroughput={
        'ReadCapacityUnits': 1, 
        'WriteCapacityUnits': 1
    }
)

table.meta.client.get_waiter('table_exists').wait(TableName='staff')
print(table.item_count)


#table = dynamodb.Table('experiments')

urlbase = 'https://s3.console.aws.amazon.com/s3/buckets/hw3-cmu/'
with open('experiments.csv', 'rt') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(csvf)
    for item in csvf:
        if item[0] is not None:
            print (item[0])
            body = open(item[4], 'rb')
            s3.Object('hw3-cmu',item[4]).Acl().put(ACL = 'public-read')
            url = urlbase + item[4]
            metadata_item = {'id' : int(item[0]), 'Temp' : item[1], 
            'Conductivity': item [2],'Concentration': item[3], 'url':url}
            table.put_item(Item = metadata_item)