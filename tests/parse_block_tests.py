from clients.refined_models import BtcAddresses
from blockchain import blockexplorer
import json


def dup_detector():
    addr_ids = [x.identifier for x in BtcAddresses.scan()]
    set_addr_ids = set(addr_ids)
    if len(addr_ids) == len(set_addr_ids):
        print("no duplicate ids")
    else:
        print("duplicate ids present")


def test_craziness():
    block_hash = '000000000000000000114f812c5ee0aa6aa6c6c63f50a5bc9158e95223350808'
    address = '1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7'

    block = blockexplorer.get_block(block_hash)

    num = 1
    for tx in block.transactions[1:]:
        output_addr = [x.address for x in tx.outputs if x.address is not None]
        if address in output_addr:
            print(num)
            break
        num += 1


def test_crazy():
    address = '1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7'
    obj = BtcAddresses.get(address)
    print(obj.used_as_input)


def test_input_tx_record():
    lst = []

    for item in BtcAddresses.scan():
        input_list = json.loads(item.used_as_input)
        lst.append((len(input_list), item.address))

    max_val = max(lst)
    print(max_val)


if __name__ == "__main__":
    dup_detector()
