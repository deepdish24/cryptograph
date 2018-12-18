from clients.models import BtcAddresses, BtcTransactions, BtcNodeIdentifier
from blockchain import blockexplorer
from pynamodb.exceptions import DoesNotExist
import json
import pickle
import time

# ====== Query amount of clustering ========


def get_clustering():
    num_nodes = BtcNodeIdentifier.count()
    num_addresses = BtcAddresses.count()
    print("addresses / nodes: " + str(num_addresses / num_nodes))

# ======== Query degree distribution ========
# adj_list = {node_id : [(neighbor, val_of_transaction), (), ...]}


def get_degree_distr():
    adj_list = {x.node_id: {} for x in BtcNodeIdentifier.scan()}

    for tx in BtcTransactions.scan():
        input_addr = [x for x in tx.inputs]
        output_addr = [(x, json.loads(tx.outputs[x])[0]) for x in tx.outputs]
        try:
            source_node_id = BtcAddresses.get(input_addr[0]).node_id
        except DoesNotExist:
            continue

        all_input_addr = BtcAddresses.batch_get(input_addr)
        node_ids = set([x.node_id for x in all_input_addr])
        if len(node_ids) > 1:
            print("TROUBLE!!!")
            print(tx.tx_hash)
            print(node_ids)
            print("========================")

        neighbor_nodes_map = adj_list[source_node_id]
        for addr, val in output_addr:

            try:
                target_node_id = BtcAddresses.get(addr).node_id
            except DoesNotExist:
                continue

            if target_node_id in neighbor_nodes_map:
                lst = neighbor_nodes_map[target_node_id]
                lst.append((val, tx.tx_hash))
                neighbor_nodes_map[target_node_id] = lst
            else:
                neighbor_nodes_map[target_node_id] = [(val, tx.tx_hash)]

    degree_distr = sorted([len(x.keys()) for x in adj_list.values()])
    with open("pickles/degree_distr.pickle", "wb") as f:
        pickle.dump(adj_list, f)
    print("max_degree:" + str(max(degree_distr)))


def write_edges_for_tx(tx, f_handle):
    tx_inputs = json.loads(tx.inputs)
    tx_outputs = json.loads(tx.outputs)

    input_addr = set(x['address'] for x in tx_inputs)
    output_addr_dct = {x['address']: (x['value'] / 100000000) for x in tx_outputs}

    input_addr_objs = BtcAddresses.batch_get(input_addr)
    output_addr_objs = BtcAddresses.batch_get(output_addr_dct.keys())
    node_ids_input = set(x.node_id for x in input_addr_objs)

    if len(node_ids_input) > 1:
        print("ERROR: the following tx inputs have differing nodes")
        return

    source_node_id = BtcAddresses.get(tx_inputs[0]['address']).node_id

    node_to_list_addr = {}

    for addr in output_addr_objs:
        node_id = addr.node_id
        if node_id in node_to_list_addr:
            lst = node_to_list_addr[node_id]
            lst.append(addr.address)
            node_to_list_addr[node_id] = lst
        else:
            node_to_list_addr[node_id] = [addr.address]

    node_obj = BtcNodeIdentifier.get(node_ids_input.pop())

    if input_addr <= node_obj.addresses:
        for output_node_id in node_to_list_addr.keys():
            lst_addrs = node_to_list_addr[output_node_id]
            value = sum([output_addr_dct[x] for x in lst_addrs])
            line = str(source_node_id) + "," + str(output_node_id) + "," + \
                   str(value) + "," + str(tx.time) + "," + str(tx.tx_inx)
            f_handle.write(line)
            f_handle.write("\n")
    else:
        print("ERROR: following node does not contain all members")


def get_adj_list_multi(output_file):
    """
    Function computes an adjacency list representation and
    writes to file passed in as input. Note that graph
    has multi-edges

    Format of adjacency list
    <src_node>,<tgt_node>,<value>,<time>,<tx_index>

    :param output_file: path of file to write adj list out to
    """

    with open(output_file, "w") as f:
        for tx in BtcTransactions.scan():
            try:
                write_edges_for_tx(tx, f)
            except Exception as e:
                print(str(e))
                time.sleep(180)


def test_node_clustering():
    for tx in BtcTransactions.scan():
        tx_inputs = json.loads(tx.inputs)
        tx_outputs = json.loads(tx.outputs)

        input_addr = set(x['address'] for x in tx_inputs)
        output_addr = set(x['address'] for x in tx_outputs)

        try:
            source_node_id = BtcAddresses.get(tx_inputs[0]['address']).node_id
        except DoesNotExist:
            print("ERROR: address %s does not exist" % tx_inputs[0]['address'])
            break

        all_input_addr = BtcAddresses.batch_get(input_addr)
        node_ids = set(x.node_id for x in all_input_addr)

        if len(node_ids) > 1:
            print("ERROR: the following tx inputs have differing nodes")
            print(tx.tx_hash)
            print(node_ids)
            break

        node_obj = BtcNodeIdentifier.get(node_ids.pop())
        if not (input_addr <= node_obj.addresses):
            print("ERROR: following node does not contain all members")
            print(tx.tx_hash)
            print(str(node_obj.node_id))
            break


def count_nodes():
    return len([x for x in BtcNodeIdentifier.scan()])


def count_num_nodes_in_file(fname, fname_new):
    with open(fname_new, "w") as f:
        with open(fname, "r") as f2:
            all_nodes = set()
            times = set()
            for line in f2.readlines():
                arr = line.split(",")
                if len(arr) > 1:
                    all_nodes.add(int(arr[0]))
                    all_nodes.add(int(arr[1]))
                    times.add(int(arr[3]))

        f.write(str(max(all_nodes)) + "\n")
        f.write(str(min(times)) + "," + str(max(times)) + "\n")

        with open(fname, "r") as f2:
            for line in f2.readlines():
                f.write(line)


def avg_clustering():
    BtcTransactions.count()


def test_weird_case():
    tx_hash = '31d1c47b2d85a61b2f3983bc760f1d8fea468159aa1724b9031455c7f1ec0160'
    tx_obj = blockexplorer.get_tx(tx_hash)
    for item in tx_obj.inputs:
        print(item.address)


if __name__ == "__main__":
    # test_node_clustering()
    # get_adj_list_multi("graphs/btc_multi.txt")
    count_num_nodes_in_file("graphs/btc_multi.txt", "graphs/btc_multi_tmp.txt")
    # test_weird_case()
    # nodes = count_nodes()
    # print(nodes)
