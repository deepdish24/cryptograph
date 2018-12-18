from dynamo_example import TestModel
from clients.models import BtcAddresses, BtcTransactions, BtcNodeIdentifier
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

nodes = [x for x in BtcNodeIdentifier.scan()]
num_nodes = len(nodes)
print("num_nodes: " + str(num_nodes))

values = [len(x.addresses) for x in BtcNodeIdentifier.scan()]
print("max clustering:" + str(max(values)))
print("avg clustering:" + str(sum(values) / len(values)))


addresses = [x for x in BtcAddresses.scan()]
print("num addresses: " + str(len(addresses)))




'''tx_obj = BtcTransactions.get("03edaffe58c6ef0bb47f8c8c25fed0d487901f759541fc42e9bb53e4d59f60a6")
print(tx_obj.outputs)

# Degree Distribution
distr = []
for item in nodes[1:10]:
    addr_objs = BtcAddresses.batch_get(item.addresses)
    used_as_input = [y for x in addr_objs for y in x.used_as_input]
    txs = BtcTransactions.batch_get(used_as_input)
    all_output_addresses = [y for x in txs for y in x.outputs.deserialize().keys()]'''


'''user = TestModel.get('test')
user.new_attr = {'k1': 'v1', 'k2': 'v2'}
user.save()'''

'''user = TestModel.get('test')
print(user.new_attr['k1'])'''

