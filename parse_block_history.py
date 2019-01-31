from blockchain import blockexplorer
import sys
import json
import time
from pynamodb.exceptions import DoesNotExist
# from models.refined_models import BtcTransactions, BtcAddresses
# from models.backtest_models import BtcTransactions, BtcAddresses
from models.testdb_models import BtcTransactions, BtcAddresses
# from query.query_helper import get_num_addresses
# from query.query_helper_backtest import get_num_addresses
from query.query_helper_testdb import get_num_addresses

CURR_ADDR_ID = get_num_addresses()
last_seen_addrs = set()
fname = ""


def write_to_file(string, fname):
    if fname != "":
        with open(fname, "a") as f:
            f.write(string + "\n")


def db_put_address_inputs(addresses, tx_index):
    """
    update address/node objects with input information
    these can't be batched together since they are dependent on each
    other
    """

    global CURR_ADDR_ID
    addr_objs = {x.address: x for x in BtcAddresses.batch_get(addresses, consistent_read=True)}

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

    addr_objs = {x.address: x for x in BtcAddresses.batch_get(addresses, consistent_read=True)}

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


def wait_for_dynamo_sync(addresses):
    """
    Since DynamoDB is eventually consistent, function waits for address objects to be
    stored in database before re-querying for object. This function is called with the
    addresses of the last three seen transactions, to make sure that no address object
    is seen and then overwritten (because database write does not have strong consistency)

    :param addresses: set of addresses to check for
    :return: function returns once all addresses are retrievable
    """
    global fname
    all_retrievable = False

    while not all_retrievable:
        can_retrieve_all = True

        for addr in addresses:
            try:
                BtcAddresses.get(addr)
            except DoesNotExist:
                can_retrieve_all = False
                break

        all_retrievable = can_retrieve_all

        if not all_retrievable:
            write_to_file("sleeping for strong consistency...", fname)
            time.sleep(10)


def db_put(block):

    global last_seen_addrs

    # reset the set of last seen addresses
    last_seen_addrs = set()

    # iterate through transactions and write to database
    with BtcTransactions.batch_write() as batch:
        ctr = 0
        for tx in block.transactions[1:]:
            try:
                BtcTransactions.get(tx.tx_index)
            except DoesNotExist:
                # list of inputs for transaction (can contain duplicates)
                valid_inputs = [x for x in tx.inputs if 'address' in x.__dict__.keys()
                                and x.address is not None]

                # list of outputs for transaction (cannot contain duplicates)
                valid_outputs = [x for x in tx.outputs if 'address' in x.__dict__.keys() and
                                 x.address is not None]

                addresses_input = set(x.address for x in valid_inputs)
                addresses_output = set(x.address for x in valid_outputs)

                input_last_seen = addresses_input & last_seen_addrs
                output_last_seen = addresses_output & last_seen_addrs

                # check for strong consistency
                if len(input_last_seen) > 0:
                    wait_for_dynamo_sync(input_last_seen)

                if len(output_last_seen) > 0:
                    wait_for_dynamo_sync(output_last_seen)

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

                ctr += 1

                # for every three addresses, reset last seen address
                if ctr % 3 == 0:
                    last_seen_addrs = addresses_input | addresses_output
                else:
                    last_seen_addrs = last_seen_addrs | addresses_input | addresses_output


def wait_and_load(block, interval_wait, num_times, log_file):
    if num_times < 5:
        try:
            db_put(block)
            return
        except Exception as e:
            write_to_file("error in parsing block: %s" % str(e), log_file)
            write_to_file("proceeding to wait...", log_file)
            time.sleep(interval_wait)
            write_to_file("sleep finished...resuming", log_file)
            wait_and_load(block, interval_wait + 60, num_times + 1, log_file)
    else:
        write_to_file("block failed...moving onto next block")
        return


def wait_and_load_no_log(block, interval_wait, num_times):
    if num_times < 5:
        try:
            db_put(block)
            return
        except Exception as e:
            print("error in parsing block: %s" % str(e))
            print("proceeding to wait...")
            time.sleep(interval_wait)
            print("sleep finished...resuming")
            wait_and_load_no_log(block, interval_wait + 60, num_times + 1)
    else:
        write_to_file("block failed...moving onto next block")
        return


def load_blocks(num_blocks, block_hash, log_file):
    global CURR_ADDR_ID
    print("starting curr addr id is: " + str(CURR_ADDR_ID))
    block = blockexplorer.get_block(block_hash)
    for i in range(num_blocks):
        print("parsing block: %s" % block.hash)
        wait_and_load(block, 60, 0, log_file)
        print("done with block: %s" % block.hash)
        block = blockexplorer.get_block(block.previous_block)


def load_from_block(block_height, log_file):
    """
    Function moves forward and loads blocks one by one, starting from given block height.
    In the case that a block contains less than 50 transactions, functions sleeps for 10 seconds
    to avoid spamming blockexplorer api endpoint
    :param block_height:
    :return: void
    """
    curr_block_height = block_height

    while True:
        # print("loading block of height %d" % curr_block_height)
        try:
            message = "loading block of height %d" % curr_block_height
            write_to_file(message, log_file)
            block = blockexplorer.get_block_height(curr_block_height)[0]
            wait_and_load(block, 60, 1, log_file)

            if block.n_tx < 50:
                write_to_file("sleeping...", log_file)
                time.sleep(10)

            curr_block_height += 1
        except Exception as e:
            write_to_file("error in retrieving block: %s" % str(e), log_file)
            write_to_file("trying again...", log_file)


def load_num_blocks(block_height, num_blocks):
    """
    Function loads num blocks starting with block of height block_height
    in blockchain

    :param block_height: starting block
    :param num_blocks: number of blocks to parse
    :return: void
    """
    curr_block_height = block_height

    for i in range(num_blocks):
        # print("loading block of height %d" % curr_block_height)
        try:
            message = "loading block of height %d" % curr_block_height
            print(message)
            block = blockexplorer.get_block_height(curr_block_height)[0]
            wait_and_load_no_log(block, 60, 1)

            if block.n_tx < 50:
                print("sleeping...")
                time.sleep(10)

            curr_block_height += 1
        except Exception as e:
            print("error in retrieving block: %s" % str(e))
            print("trying again...")


def load_single_block(block_hash):
    print("loading single block...")
    block2 = blockexplorer.get_block(block_hash)
    block = blockexplorer.get_block_height(559684)[0]
    print("block hash: " + str(block2.hash))
    print("block hash from height: " + str(block.hash))
    # wait_and_load(block, 0, 3)


if __name__ == "__main__":
    if sys.argv[1] == "--cap":
        block_height = int(sys.argv[2])
        num_blocks = int(sys.argv[3])
        load_num_blocks(block_height, num_blocks)
    elif sys.argv[1] == "--single":
        block_hash = sys.argv[2]
        load_single_block(block_hash)
    else:
        block_height = int(sys.argv[1])
        fname = sys.argv[2]
        load_from_block(block_height, fname)
