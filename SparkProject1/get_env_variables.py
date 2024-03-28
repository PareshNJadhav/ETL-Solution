import os

os.environ['env'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
env = os.environ['env']

app_name = 'Pyspark App'

current_path = os.getcwd()
source_olap = current_path+ '/source/olap'
source_oltp = current_path+ '/source/oltp'
properties_path = current_path+ '/properties/config'
