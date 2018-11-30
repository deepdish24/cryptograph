from dynamo_example import TestModel
from clients.models import BtcAddresses, BtcTransactions
from pynamodb.exceptions import DoesNotExist, PutError

'''user = TestModel(hash='deepans@seas', value='test', friends=[])
user2 = TestModel(hash='test', value='test', friends=['cool', 'beans'],
                  new_attr={'k1': 'v1', 'k2': 'v2'})

user.save()
user2.save()'''

'''try:
    user = BtcAddresses.get("cool")
    print(user)
except DoesNotExist:
    print("empty")'''

'''count = 0
for item in BtcTransactions.scan():
    count+=1
print(count)'''

for item in BtcTransactions.scan():
    print(item.tx_hash)
    # item.delete()

'''user = TestModel.get('test')
user.new_attr = {'k1': 'v1', 'k2': 'v2'}
user.save()'''

'''user = TestModel.get('test')
print(user.new_attr['k1'])'''

