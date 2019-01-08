from blockchain import blockexplorer
import json
from pynamodb.exceptions import DoesNotExist, GetError
from query.query_helper import get_num_addresses
import time
from models.models import BtcTransactions, BtcNodeIdentifier, BtcAddresses

# CURR_NODE_ID = get_num_addresses()


def test_explorer_call():
    block = blockexplorer\
        .get_block('0000000000000000001ae8a8094c0cbd4e2db822235f62af328b100d14e2acfe')
    for tx in block.transactions[1:2]:
        print(tx.__dict__)
        print(len(tx.inputs))
        print(tx.hash)
        for tinput in tx.inputs:
            if 'address' in tinput.__dict__:
                print(tinput.__dict__)
        for toutput in tx.outputs:
            print(toutput.__dict__)


def coalesce_nodes(mismatched_nodes, true_node_id, new_addrs_in_tx):
    """
    Function clusters nodes of shared addresses into the node
    identified by the true_node_id
    (i.e. id of the address with lowest node id value)

    :param mismatched_nodes: nodes that are to be clustered together
    :param true_node_id: node we are clustering into
    :param new_addrs_in_tx: multi input transaction where some nodes need to be
    coalesced but some new addresses need to be created and associated with the
    same true_node_id
    :return: void
    """
    node_objs = [x for x in BtcNodeIdentifier.batch_get(mismatched_nodes)]
    addresses_to_update = [y for x in node_objs for y in x.addresses]

    # update node_id of address objects
    try:
        for addr_obj in BtcAddresses.batch_get(addresses_to_update):
            addr_obj.node_id = true_node_id
            addr_obj.save()
    except GetError:
        print("GET ERROR")
        print(node_objs)
        print("===========================")
        print(addresses_to_update)

    true_node_obj = BtcNodeIdentifier.get(true_node_id)

    for addr in addresses_to_update:
        true_node_obj.addresses.add(addr)

    for addr in new_addrs_in_tx:
        true_node_obj.addresses.add(addr)

    true_node_obj.save()

    for item in node_objs:
        item.addresses = set([])
        item.save()
        item.delete()


def db_put_address_inputs(addresses, tx_hash):
    """
    update address/node objects with input information
    these can't be batched together since they are dependent on each
    other
    """
    # step1: find common node id
    global CURR_NODE_ID

    '''if '1Lx9wpR4CRRCtRFJK3mHNsPojxWsBYCWPP' in addresses:
        print("IN INPUT:")
        print(tx_hash)
        print(addresses)
        print("============================")'''

    # get all input addresses and corresponding node_id
    addr_objs = {x.address: x for x in BtcAddresses.batch_get(addresses)}
    new_addrs_in_tx = [x for x in addresses if x not in addr_objs]
    node_ids = set(item.node_id for item in addr_objs.values())

    '''if '1Lx9wpR4CRRCtRFJK3mHNsPojxWsBYCWPP' in addresses:
        print(addr_objs)
        print(new_addrs_in_tx)
        print(node_ids)
        print('-----------------------------------')'''

    if len(node_ids) != 0:
        # take smallest node_id to be identifier for all nodes
        node_id = node_ids.pop()
        # note that this only coalesces existing addresses
        coalesce_nodes(node_ids, node_id, new_addrs_in_tx)
    else:
        # if no nodes for any of the addresses exist, then create node object
        node_id = CURR_NODE_ID
        CURR_NODE_ID += 1
        node_obj = BtcNodeIdentifier(node_id=node_id, addresses=addresses)
        node_obj.save()

    # referesh the addr_objs object
    addr_objs = {x.address: x for x in BtcAddresses.batch_get(addresses)}

    '''if '1Lx9wpR4CRRCtRFJK3mHNsPojxWsBYCWPP' in addresses:
        print(addr_objs)
        print(addresses)
        print('-----------------------------------')'''

    with BtcAddresses.batch_write() as batch:
        for address in addresses:
            if address in addr_objs:
                address_obj = addr_objs[address]
                '''if address == '1Lx9wpR4CRRCtRFJK3mHNsPojxWsBYCWPP':
                    print("INPUT: this can't be possible!!")
                    print(tx_hash)
                    print("=============================")'''
                address_obj.used_as_input.add(tx_hash)
                batch.save(address_obj)
            else:
                # first time seeing this address so use node_id decided on above
                address_obj = BtcAddresses(address=address,
                                           node_id=node_id,
                                           used_as_input=set([tx_hash]),
                                           used_as_output=set([]))

                '''if address == '1Lx9wpR4CRRCtRFJK3mHNsPojxWsBYCWPP':
                    print("INPUT: New address obj created for address BEFORE MAP ADD")
                    print(address_obj.used_as_output)
                    print("--------------------------------")'''

                addr_objs[address] = address_obj

                '''if address == '1Lx9wpR4CRRCtRFJK3mHNsPojxWsBYCWPP':
                    print("INPUT: New address obj created for address")
                    print(address_obj.used_as_output)
                    print(tx_hash)
                    print("==============================")'''

                batch.save(address_obj)


def db_put_address_outputs(addresses, tx_hash):
    """
    update address/node objects with output information
    these can't be batched together since they are dependent on each
    other
    """
    global CURR_NODE_ID

    addr_objs = {x.address: x for x in BtcAddresses.batch_get(addresses)}

    '''if '1Lx9wpR4CRRCtRFJK3mHNsPojxWsBYCWPP' in addresses:
        print("IN OUTPUT:")
        print(tx_hash)
        print(addresses)
        print("============================")'''

    with BtcAddresses.batch_write() as batch_addr:
        for address in addresses:
            if address in addr_objs:
                address_obj = addr_objs[address]
                '''if address == '1Lx9wpR4CRRCtRFJK3mHNsPojxWsBYCWPP':
                    print("OUTPUT: this can't be possible!!")
                    print(tx_hash)
                    print("==============================")'''
                address_obj.used_as_output.add(tx_hash)
                batch_addr.save(address_obj)
            else:
                # first time seeing this address, so create node_id for it
                node_id = CURR_NODE_ID
                CURR_NODE_ID += 1
                node_obj = BtcNodeIdentifier(node_id=node_id, addresses=set([address]))
                node_obj.save()

                address_obj = BtcAddresses(address=address,
                                           node_id=node_id,
                                           used_as_output=set([tx_hash]),
                                           used_as_input=set([]))
                '''if address == '1Lx9wpR4CRRCtRFJK3mHNsPojxWsBYCWPP':
                    print("OUTPUT: new address obj created for address")
                    print(address_obj.used_as_output)
                    print(tx_hash)
                    print("==============================")'''
                addr_objs[address] = address_obj
                batch_addr.save(address_obj)


def db_put(block):
    # iterate through transactions and write to database
    with BtcTransactions.batch_write() as batch:
        for tx in block.transactions[1:]:
            try:
                BtcTransactions.get(tx.hash)
            except DoesNotExist:
                # list of inputs for transaction (can contain duplicates)
                valid_inputs = [x for x in tx.inputs if x.address is not None]

                # list of outputs for transaction (cannot contain duplicates)
                valid_outputs = [x for x in tx.outputs if x.address is not None]

                # input -> data map (only uses latest value in the case of duplicates in inputs)
                input_list = []
                for input_obj in valid_inputs:
                    data = {
                        'address': input_obj.address,
                        'value': input_obj.value,
                        'tx_inx': input_obj.tx_index
                    }
                    input_list.append(data)

                output_list = []
                for output_obj in valid_outputs:
                    data = {
                        'address': output_obj.address,
                        'value': output_obj.value
                    }
                    output_list.append(data)

                total_input = sum([x.value for x in tx.inputs])
                total_output = sum([x.value for x in tx.outputs])

                # create transaction object
                tx_object = BtcTransactions(tx_hash=tx.hash,
                                            time=tx.time,
                                            total_val_input=total_input,
                                            total_val_output=total_output,
                                            tx_inx=tx.tx_index,
                                            inputs=json.dumps(input_list),
                                            outputs=json.dumps(output_list)
                                            )
                batch.save(tx_object)

                db_put_address_inputs(set(x.address for x in valid_inputs), tx.hash)
                db_put_address_outputs(set(x.address for x in valid_outputs), tx.hash)


def wait_and_load(block, interval_wait, num_times):
    if num_times < 5:
        try:
            db_put(block)
            return
        except Exception as e:
            print("error in parsing block: %s" % str(e))
            print("proceeding to wait...")
            time.sleep(interval_wait)
            print("sleep finished...resuming")
            wait_and_load(block, interval_wait + 60, num_times + 1)
    else:
        print("block failed...moving onto next block")
        return


def load_blocks(num_blocks, block_hash):
    block = blockexplorer.get_block(block_hash)
    for i in range(num_blocks):
        print("parsing block: %s" % block.hash)
        wait_and_load(block, 60, 0)
        print("done with block: %s" % block.hash)
        block = blockexplorer.get_block(block.previous_block)


def load_single_block(block_hash):
    block = blockexplorer.get_block(block_hash)
    db_put(block)


if __name__ == "__main__":
    # block_hash = sys.argv[1]
    # load_single_block(block_hash)
    # load_blocks(30, block_hash)
    test_explorer_call()

