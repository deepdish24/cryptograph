import json
import asyncio
import websockets
from blockchain import blockexplorer
from pynamodb.exceptions import DoesNotExist
from query.query_helper import get_num_addresses
from models.refined_models import BtcAddresses, BtcTransactions

CURR_ADDR_ID = get_num_addresses()


def db_put_address_inputs(addresses, tx_index):
    """
    update address/node objects with input information
    these can't be batched together since they are dependent on each
    other
    """

    global CURR_ADDR_ID
    addr_objs = {x.address: x for x in BtcAddresses.batch_get(addresses)}

    for address in addresses:
        if address in addr_objs:
            address_obj = addr_objs[address]
            input_set = set(json.loads(address_obj.used_as_input))
            input_set.add(tx_index)
            address_obj.used_as_input = json.dumps(list(input_set))
        else:
            # first time seeing this address so use node_id decided on above
            address_id = CURR_ADDR_ID
            CURR_ADDR_ID += 1
            address_obj = BtcAddresses(address=address,
                                       identifier=address_id,
                                       used_as_input=json.dumps([tx_index]))

        addr_objs[address] = address_obj

    # update neighbor addresses of all input addresses for clustering
    identifier_set = set(x.identifier for x in addr_objs.values())

    with BtcAddresses.batch_write() as batch:
        for address in addresses:
            # for each input address, add all other nodes to set of identifiers
            address_obj = addr_objs[address]
            node_identifier_set = set(json.loads(address_obj.neighbor_addrs))
            new_addr_identifiers = identifier_set.difference(set([address_obj.identifier]))
            address_obj.neighbor_addrs = \
                json.dumps(list(node_identifier_set.union(new_addr_identifiers)))

            batch.save(address_obj)

    return addr_objs


def db_put_address_outputs(addresses, tx_index):
    """
    update address/node objects with output information
    these can't be batched together since they are dependent on each
    other
    """
    global CURR_ADDR_ID

    addr_objs = {x.address: x for x in BtcAddresses.batch_get(addresses)}

    with BtcAddresses.batch_write() as batch:
        for address in addresses:
            if address in addr_objs:
                address_obj = addr_objs[address]

                output_set = set(json.loads(address_obj.used_as_output))
                output_set.add(tx_index)

                address_obj.used_as_output = json.dumps(list(output_set))
                batch.save(address_obj)
            else:
                # first time seeing this address, so create node_id for it
                address_id = CURR_ADDR_ID
                CURR_ADDR_ID += 1

                address_obj = BtcAddresses(address=address,
                                           identifier=address_id,
                                           used_as_output=json.dumps([tx_index]))
                addr_objs[address] = address_obj
                batch.save(address_obj)

    return addr_objs


def db_put(block):
    # iterate through transactions and write to database
    with BtcTransactions.batch_write() as batch:
        for tx in block.transactions[1:]:
            try:
                BtcTransactions.get(tx.tx_index)
            except DoesNotExist:
                # list of inputs for transaction (can contain duplicates)
                valid_inputs = [x for x in tx.inputs if x.address is not None]

                # list of outputs for transaction (cannot contain duplicates)
                valid_outputs = [x for x in tx.outputs if x.address is not None]

                addresses_input = set(x.address for x in valid_inputs)
                addresses_output = set(x.address for x in valid_outputs)

                # add addresses to database and/or update address tx info
                addr_objs_input = db_put_address_inputs(addresses_input, tx.tx_index)
                addr_objs_output = db_put_address_outputs(addresses_output, tx.tx_index)

                input_list = []
                for input_obj in valid_inputs:
                    data = {
                        'address': addr_objs_input[input_obj.address].identifier,
                        'value': input_obj.value,
                        'tx_inx': input_obj.tx_index
                    }
                    input_list.append(data)

                output_list = []
                for output_obj in valid_outputs:
                    data = {
                        'address': addr_objs_output[output_obj.address].identifier,
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


def load_single_block(block_hash):
    print("parsing block %s" % block_hash)
    block = blockexplorer.get_block(block_hash)
    try:
        db_put(block)
    except Exception as e:
        print("error in parsing block %s" % block_hash)
        print(str(e))


async def client_main():
    """
    function that subscribes to block creation events and adds
    round-robins transactions to subprocess that parse and add transaction to
    database
    :return: void
    """
    async with websockets.connect(
            'wss://ws.blockchain.info/inv') as websocket:
        ping_msg = json.dumps({'op':'ping'})
        sub_blocks_msg = json.dumps({'op':'blocks_sub'})

        await websocket.send(ping_msg)
        await websocket.send(sub_blocks_msg)

        while True:
            messages = await websocket.recv()
            block_info = json.loads(messages)
            print(block_info[''])
            print(messages)

asyncio.get_event_loop().run_until_complete(client_main())
