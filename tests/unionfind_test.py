from unionfind import unionfind
from python_algorithms.basic.union_find import UF


def test_graph_1():
    txs = [[1, 2, 3], [4, 5], [4, 6], [5, 7]]
    struct = unionfind(8)

    for tx in txs:
        parent = min(tx)
        for node in tx:
            struct.unite(parent, node)

    return struct.groups()


def test_graph_2():
    txs = [[1, 2, 3], [4, 5], [4, 6], [5, 7]]
    uf = UF(8)

    for tx in txs:
        parent = min(tx)
        for node in tx:
            uf.union(parent, node)

    print(uf.connected(5, 6))
    print(uf.find(4))
    print(uf.find(7))
    print(uf.find(3))
    return uf.count()


if __name__ == "__main__":
    print(test_graph_2())