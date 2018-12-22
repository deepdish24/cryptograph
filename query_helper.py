from clients.refined_models import BtcTransactions, BtcAddresses
from blockchain import blockexplorer


def get_num_addresses():
    addr_ids = [x.identifier for x in BtcAddresses.scan()]
    if len(addr_ids) == 0:
        return 0
    return max(addr_ids) + 1


def test_craziness():
    block_hash = '000000000000000000114f812c5ee0aa6aa6c6c63f50a5bc9158e95223350808'
    address = '1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7'

    block = blockexplorer.get_block(block_hash)

    for tx in block.transactions[1:]:
        input_addrs = [x.address for x in tx.inputs]
        if address in input_addrs:
            print(tx.hash)

test_craziness()

