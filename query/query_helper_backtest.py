from models.backtest_models import BtcAddresses


def get_num_addresses():
    addr_ids = [x.identifier for x in BtcAddresses.scan()]
    if len(addr_ids) == 0:
        return 0
    return max(addr_ids) + 1


def address_check():
    addr_dct = {x.identifier: x for x in BtcAddresses.scan()}
    addr_obj = addr_dct[205202]
    print(addr_obj.__dict__)


def find_address_for_identifier(identifier):
    for address_obj in BtcAddresses.scan():
        if address_obj.identifier == identifier:
            print(address_obj.address)

    print("Done!")


if __name__ == "__main__":
    print("num addresses: " + str(get_num_addresses()))
