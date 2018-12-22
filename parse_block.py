from blockchain import blockexplorer
import sys
import json
import time
from pynamodb.exceptions import PutError, DoesNotExist, GetError
from clients.refined_models import BtcTransactions, BtcAddresses
from query_helper import get_num_addresses

CURR_ADDR_ID = get_num_addresses()


def db_put_address_inputs(addresses, tx_index):
    """
    update address/node objects with input information
    these can't be batched together since they are dependent on each
    other
    """

    global CURR_ADDR_ID
    addr_objs = {x.address: x for x in BtcAddresses.batch_get(addresses)}

    if '1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7' in addr_objs:
        print("used_as_input check before addition")
        print(addr_objs['1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7'].used_as_input)
        print("========================================================")

    for address in addresses:
        if address in addr_objs:
            if address == '1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7':
                print("addres 1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7 being added to INPUT")
                print(tx_index)
                print("=======================================================")
            address_obj = addr_objs[address]
            address_obj.used_as_input.add(tx_index)
        else:
            # first time seeing this address so use node_id decided on above
            address_id = CURR_ADDR_ID
            CURR_ADDR_ID += 1
            address_obj = BtcAddresses(address=address,
                                       identifier=address_id,
                                       used_as_input=set([tx_index]),
                                       used_as_output=set([]),
                                       neighbor_addrs=set([]))
        addr_objs[address] = address_obj

    # update neighbor addresses of all input addresses for clustering
    identifier_set = set(x.identifier for x in addr_objs.values())

    for address in addresses:
        address_obj = addr_objs[address]
        new_addr_identifiers = identifier_set.difference(set([address_obj.identifier]))
        address_obj.neighbor_addrs = address_obj.neighbor_addrs.union(new_addr_identifiers)

        if address == '1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7':
            print(address_obj.used_as_input)

        address_obj.save()

    return addr_objs


def db_put_address_outputs(addresses, tx_index):
    """
    update address/node objects with output information
    these can't be batched together since they are dependent on each
    other
    """
    global CURR_ADDR_ID

    addr_objs = {x.address: x for x in BtcAddresses.batch_get(addresses)}

    for address in addresses:
        if address in addr_objs:
            address_obj = addr_objs[address]
            address_obj.used_as_output.add(tx_index)
            address_obj.save()
        else:
            # first time seeing this address, so create node_id for it

            if address == '1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7':
                print("addres 1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7 being added to OUTPUT")
                print(tx_index)
                print("=======================================================")

            address_id = CURR_ADDR_ID
            CURR_ADDR_ID += 1

            address_obj = BtcAddresses(address=address,
                                       identifier=address_id,
                                       used_as_output=set([tx_index]),
                                       used_as_input=set([]),
                                       neighbor_addrs=set([]))
            addr_objs[address] = address_obj

            if '1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7' == address:
                print("used_as_input check right after object creation OUTPUT")
                print(addr_objs[address].used_as_input)
                print("========================================================")

            address_obj.save()

    if '1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7' in addresses:
        time.sleep(5)
        address_obj = BtcAddresses.get('1GnbyZEnmc32MtZdd2n5FjwhvxsZ1Vk4z7')
        print("test of used_as_input after saving to OUTPUT")
        print(address_obj.used_as_input)

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
    block_hash = sys.argv[1]
    load_single_block(block_hash)
    # load_blocks(30, block_hash)