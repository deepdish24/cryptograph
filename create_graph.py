from models.refined_models import BtcAddresses, BtcTransactions
from python_algorithms.basic.union_find import UF
from query.query_helper import get_num_addresses
import pickle
import json
import sys


def get_cc(recompute=False):
    if not recompute:
        with open("pickles/cc.pickle", "rb") as f:
            dct = pickle.load(f)

        return dct, len(dct.keys())

    print("identifying nodes...")
    addr_dct = {x.identifier: x for x in BtcAddresses.scan()}
    addr_identifiers = addr_dct.keys()
    max_id = get_num_addresses()
    print("num addresses: " + str(len(addr_identifiers)))
    union_find = UF(max_id)

    for identifier, address_struct in addr_dct.items():
        neighbor_addrs = json.loads(address_struct.neighbor_addrs)
        parent = identifier
        if len(neighbor_addrs) != 0:
            for neighbor in neighbor_addrs:
                union_find.union(neighbor, parent)

    print("===== union find done =====")
    print("normalizing node ids...")

    # need to re-number groups so that node numbers go from 0 - num_connected_components

    init_node_id = 0

    # addr to curr_node_id
    addr_to_node_id = {}

    # pre_node_id to curr_node_id
    used_node_ids = {}

    for identifier in addr_identifiers:
        node_id_pre = union_find.find(identifier)
        if node_id_pre in used_node_ids:
            node_id_post = used_node_ids[node_id_pre]
        else:
            used_node_ids[node_id_pre] = init_node_id
            node_id_post = used_node_ids[node_id_pre]
            init_node_id += 1

        addr_to_node_id[identifier] = node_id_post

    with open("pickles/cc.pickle", "wb") as handle:
        pickle.dump(addr_to_node_id, handle)

    print("cc pickled, creating graph...")

    return addr_to_node_id, len(addr_to_node_id.keys())


def create_entire_graph(graph_file):
    # we first create mapping of address identifier to node_id so that
    # clustered graph can be created. Additional clustering metrics will
    # be added in here later (TODO)

    addr_to_node_id, num_components = get_cc()

    print("creating graph...")

    with open(graph_file, "w") as f:
        f.write(str(num_components) + "\n")
        for tx in BtcTransactions.scan():
            tx_inputs = json.loads(tx.inputs)
            tx_outputs = json.loads(tx.outputs)

            input_addr = set(x['address'] for x in tx_inputs)
            output_addr_dct = {x['address']: (x['value'] / 100000000) for x in tx_outputs}

            all_node_ids = set()

            for addr in input_addr:
                try:
                    node_id = addr_to_node_id[addr]
                    # wtf...why would this happen at all
                except KeyError as e:
                    print("Key Error")
                    continue
                all_node_ids.add(node_id)

            # check to see if all input address are clustered properly
            # error here means error with union-find step above
            if len(all_node_ids) > 1:
                print("ERROR: Input address not clustered properly for tx: %s" % tx.tx_hash)
                print("inputs: " + str(input_addr))
                print("all cc: " + str(all_node_ids))
                continue

            if len(all_node_ids) == 0:
                continue

            src_node_id = all_node_ids.pop()

            tgt_nodes = []
            for addr, val in output_addr_dct.items():
                try:
                    node_id = addr_to_node_id[addr]
                except KeyError as e:
                    print("Key Error")
                    continue

                tgt_nodes.append((node_id, val))

            output_node_to_value = {}

            # output nodes can be duplicated, since different output addresses can correspond to the
            # same node. Thus, we group common nodes and sum their output values
            for node, value in tgt_nodes:
                if node in output_node_to_value:
                    output_node_to_value[node] += value
                else:
                    output_node_to_value[node] = value

            for output_node, value in output_node_to_value.items():
                line = str(src_node_id) + "," + str(output_node) + "," + \
                       str(value) + "," + str(tx.time) + "," + str(tx.tx_inx)
                f.write(line + "\n")


if __name__ == "__main__":
    filepath = sys.argv[1]
    create_entire_graph(filepath)
