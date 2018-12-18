from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, UnicodeSetAttribute, JSONAttribute
)
import argparse


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
        table_name = 'btc_transactions'
    tx_hash = UnicodeAttribute(hash_key=True)
    time = NumberAttribute()
    total_val_input = NumberAttribute()
    total_val_output = NumberAttribute()
    tx_inx = NumberAttribute()
    #  [{'address': xyx, 'value': 10, 'tx_inx': inx}, {'address': xyx, 'value': 10, 'tx_inx': inx}]
    inputs = JSONAttribute()
    # [{'address': xyx, 'value': 10}, {'address': xyx, 'value': 10}, ...]
    outputs = JSONAttribute()


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
        table_name = 'btc_addresses'
    address = UnicodeAttribute(hash_key=True)
    node_id = NumberAttribute()
    used_as_input = UnicodeSetAttribute(default=set([]))
    used_as_output = UnicodeSetAttribute(default=set([]))


class BtcNodeIdentifier(Model):
    """
    Class representing mapping from node identifier to list of addresses
    that belong to the node. We think of this as the all the addresses
    a particular user controls
    """

    class Meta:
        table_name = 'btc_node_ids'
    node_id = NumberAttribute(hash_key=True)
    addresses = UnicodeSetAttribute(default=set([]))


def delete_tables():
    BtcTransactions.delete_table()
    BtcAddresses.delete_table()
    BtcNodeIdentifier.delete_table()


def create_tables():
    BtcTransactions.create_table(wait=True, read_capacity_units=15, write_capacity_units=20)
    BtcAddresses.create_table(wait=True, read_capacity_units=25, write_capacity_units=50)
    BtcNodeIdentifier.create_table(wait=True, read_capacity_units=25, write_capacity_units=50)


def clear_tables():

    for item in BtcNodeIdentifier.scan():
        item.delete()

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
