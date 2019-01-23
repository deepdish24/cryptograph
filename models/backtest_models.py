from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, NumberSetAttribute, JSONAttribute
)
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
import argparse
import json


class BtcTransactions(Model):
    """
    Class representing information for a single bitcoin transaction.
    Each field is explained below

    tx_hash: hash of transaction (string)
    time: time of transaction (int)
    total_val_input: total amount of satoshis sent as input
    total_val_output: total amount of satoshis sent as output
    tx_inx: transaction index (int)
    input: set wallet addresses that are inputs to transaction
    output: set wallet addresses that are outputs to transaction
    """

    class Meta:
        table_name = 'btc_transactions_bt'
        read_capacity_units = 15
        write_capacity_units = 20
    tx_inx = NumberAttribute(hash_key=True)
    tx_hash = UnicodeAttribute()
    time = NumberAttribute()
    total_val_input = NumberAttribute()
    total_val_output = NumberAttribute()
    #  [{'address': xyx, 'value': 10, 'tx_inx': inx}, {'address': xyx, 'value': 10, 'tx_inx': inx}]
    inputs = JSONAttribute()
    # [{'address': xyx, 'value': 10}, {'address': xyx, 'value': 10}, ...]
    outputs = JSONAttribute()


class AddrIdentifierIndex(GlobalSecondaryIndex):
    """
    Class representing a global secondary index on
    identifier associated with each address
    """

    class Meta:
        index_name = "addr_identifier_index_bt"
        read_capacity_units = 25
        write_capacity_units = 50
        projection = AllProjection()

    identifier = NumberAttribute(hash_key=True)


class BtcAddresses(Model):
    """
    Class representing information relevant to a single bitcoin wallet address.
    Each field is explained below

    address: wallet address (string)
    node_id: id of node associated with address (after initial clustering based in common inputs) (string)
    used_as_input: set of tx_hash objects where the address was used an input
        in corresponding transaction (set)
    used_as_output: set of tx_hash objects where the address was used as output
        in corresponding transaction (set)
    """

    class Meta:
        table_name = 'btc_addresses_bt'
        read_capacity_units = 25
        write_capacity_units = 50
    address = UnicodeAttribute(hash_key=True)

    addr_index = AddrIdentifierIndex()
    identifier = NumberAttribute()

    neighbor_addrs = JSONAttribute(default=json.dumps([]))
    used_as_input = JSONAttribute(default=json.dumps([]))
    used_as_output = JSONAttribute(default=json.dumps([]))


def delete_tables():
    BtcTransactions.delete_table()
    BtcAddresses.delete_table()


def create_tables():
    BtcTransactions.create_table(wait=True)
    BtcAddresses.create_table(wait=True)


def clear_tables():
    for item in BtcAddresses.scan():
        item.delete()

    for item in BtcTransactions.scan():
        item.delete()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--delete', action='store_true')
    parser.add_argument('--create', action='store_true')
    parser.add_argument('--clear', action='store_true')
    args = parser.parse_args()

    if args.delete:
        delete_tables()

    if args.create:
        create_tables()

    if args.clear:
        clear_tables()
