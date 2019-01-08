from models.models import BtcAddresses, BtcTransactions, BtcNodeIdentifier
from blockchain import blockexplorer
from pynamodb.exceptions import DoesNotExist
import numpy as np
from collections import Counter
import matplotlib.rcsetup as rcsetup
import matplotlib.pyplot as plt
import numpy as np
import json
import pickle
import operator
import time
import sys


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


def get_adj_list(filename):
    with open(filename, "r") as f:
        num_nodes = int(f.readline())

        adj_list = {x: [] for x in range(num_nodes)}

        for line in f.readlines():
            arr = line.split(",")

            src = int(arr[0])
            tgt = int(arr[1])
            val = float(arr[2])

            lst = adj_list[src]
            lst.append((tgt, val))
            adj_list[src] = lst
    return adj_list


def get_degree_distr(filename):
    adj_list = get_adj_list(filename)
    degree_distr = [len(y) for x, y in adj_list.items()]

    hist_counter = Counter(degree_distr)
    hist_dct = dict(hist_counter.items())
    bar_heights = hist_dct.values()
    indices = np.arange(len(bar_heights))

    print(hist_dct)
    print("largest degree: " + str(max(hist_dct.keys())))
    most_occuring_degree = max(hist_dct.items(), key=operator.itemgetter(1))
    print("most occuring degree: " + str(most_occuring_degree))


    '''plt.bar(indices, bar_heights)
    plt.xlabel('Value')
    plt.ylabel('Frequency')
    plt.title('Degree Distribution')
    # maxfreq = n.max()
    # plt.ylim(ymax=np.ceil(maxfreq / 10) * 10 if maxfreq % 10 else maxfreq + 10)
    plt.show()'''


def get_graph_above_threshold(filename):
    pass


def get_scc(filename):
    pass


if __name__ == "__main__":
    filename = sys.argv[1]
    get_degree_distr(filename)
