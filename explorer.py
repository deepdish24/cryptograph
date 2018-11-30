from blockchain import blockexplorer
import sys
import json
from pynamodb.exceptions import PutError, DoesNotExist
from clients.models import BtcTransactions, BtcNodeIdentifier, BtcAddresses

CURR_NODE_ID = 0


def test_explorer_call():
    block = blockexplorer\
        .get_block('0000000000000000001ae8a8094c0cbd4e2db822235f62af328b100d14e2acfe')
    print(block.n_tx)
    #print(block.__dict__.keys())
    #print("===========================")
    for tx in block.transactions[1:2]:
        print(tx.__dict__)
        print(len(tx.inputs))
        print(tx.hash)
        for tinput in tx.inputs:
            if 'address' in tinput.__dict__:
                print(tinput.__dict__)
        print("----------------------------------")
        for toutput in tx.outputs:
            print(toutput.__dict__)
        print("=============================")
        '''print(tx.hash)
        print(tx.size)
        inputs = [(x.address, x.value) for x in tx.inputs if 'address' in x.__dict__]
        outputs = [(x.address, x.value) for x in tx.outputs]
        print(inputs)
        print(outputs)
        print('======================')'''


def coalesce_nodes(mismatched_nodes, true_node_id):
    """
    Function clusters nodes of shared addresses into the node
    identified by the true_node_id
    (i.e. id of the address with lowest node id value)

    :param mismatched_nodes: nodes that are to be clustered together
    :param true_node_id: node we are clustering into
    :return: void
    """
    node_objs = BtcNodeIdentifier.batch_get(mismatched_nodes)
    addresses_to_update = [y for x in node_objs for y in x.addresses]

    with BtcAddresses.batch_write() as batch:
        for addr_obj in BtcAddresses.batch_get(addresses_to_update):
            addr_obj.node_id = true_node_id
            batch.save(addr_obj)

    true_node_obj = BtcNodeIdentifier.get(true_node_id)
    for addr in addresses_to_update:
        true_node_obj.addresses.add(addr)
    true_node_obj.save()

    for item in node_objs:
        item.delete()


def db_put_address_inputs(addresses, tx_hash):
    """
    update address/node objects with input information
    these can't be batched together since they are dependent on each
    other
    """
    # step1: find common node id
    global CURR_NODE_ID

    # get all input addresses and corresponding node_id
    addr_values = set(x.address for x in addresses)
    addr_objs = {x.address: x for x in BtcAddresses.batch_get(addr_values)}
    node_ids = set(item.node_id for item in addr_objs.values())

    if len(node_ids) != 0:
        # take smallest node_id to be identifier for all nodes
        node_ids = sorted(node_ids)
        node_id = node_ids.pop()
        mismatched_nodes = [x for x in node_ids if x != node_id]
        coalesce_nodes(mismatched_nodes, node_id)
    else:
        # if no nodes for any of the addresses exist, then create node object
        node_id = CURR_NODE_ID
        CURR_NODE_ID += 1
        node_obj = BtcNodeIdentifier(node_id=node_id, addresses=addr_values)
        node_obj.save()

    with BtcAddresses.batch_write() as batch:
        for tx_input in addresses:
            address = tx_input.address
            if address in addr_objs:
                # address_obj = BtcAddresses.get(tx_input.address)
                address_obj = addr_objs[address]
                address_obj.used_as_input.add(tx_hash)
                batch.save(addr_objs)
            else:
                # first time seeing this address so use node_id decided on above
                address_obj = BtcAddresses(address=address,
                                           node_id=node_id,
                                           used_as_input=set([tx_hash]))
                addr_objs[address] = address_obj
                batch.save(address_obj)


def db_put_address_outputs(addresses, tx_hash):
    """
    update address/node objects with output information
    these can't be batched together since they are dependent on each
    other
    """
    global CURR_NODE_ID

    addr_values = set(x.address for x in addresses)
    addr_objs = {x.address: x for x in BtcAddresses.batch_get(addr_values)}

    with BtcAddresses.batch_write() as batch_addr, BtcNodeIdentifier.batch_write() as batch_node:
        for tx_output in addresses:
            address = tx_output.address
            if address in addr_objs:
                # address_obj = BtcAddresses.get(tx_output.address)
                address_obj = addr_objs[address]
                address_obj.used_as_output.add(tx_hash)
                batch_addr.save(address_obj)
            else:
                # first time seeing this address, so create node_id for it
                node_id = CURR_NODE_ID
                CURR_NODE_ID += 1
                node_obj = BtcNodeIdentifier(node_id=node_id, addresses=set([tx_output.address]))
                batch_node.save(node_obj)

                address_obj = BtcAddresses(address=address,
                                           node_id=node_id,
                                           used_as_output=set([tx_hash]))
                addr_objs[address] = address_obj
                batch_addr.save(address_obj)


def db_put(b_hash):
    block = blockexplorer.get_block(b_hash)
    with BtcTransactions.batch_write() as batch:
        for tx in block.transactions[1:]:
            valid_inputs = [x for x in tx.inputs if x.address is not None]
            valid_outputs = [x for x in tx.outputs if x.address is not None]

            input_map = {x.address: json.dumps([x.value, x.tx_index]) for x in valid_inputs}
            output_map = {x.address: json.dumps([x.value, x.tx_index]) for x in valid_outputs}

            total_input = sum([x.value for x in tx.inputs])
            total_output = sum([x.value for x in tx.outputs])

            # create transaction object
            tx_object = BtcTransactions(tx_hash=tx.hash,
                                        time=tx.time,
                                        total_val_input=total_input,
                                        total_val_output=total_output,
                                        tx_inx=tx.tx_index,
                                        inputs=input_map,
                                        outputs=output_map
                                        )
            batch.save(tx_object)

            db_put_address_inputs(valid_inputs, tx.hash)
            db_put_address_outputs(valid_outputs, tx.hash)


if __name__ == "__main__":
    block_hash = sys.argv[1]
    db_put(block_hash)
