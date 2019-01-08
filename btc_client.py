import asyncio
import websockets
import json
from query.query_helper import get_num_addresses

CURR_ADDR_ID = get_num_addresses()


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
        # sub_transactions_msg = json.dumps({'op':'unconfirmed_sub'})

        await websocket.send(ping_msg)
        await websocket.send(sub_blocks_msg)

        while True:
            messages = await websocket.recv()
            block_info = json.loads(messages)
            print(block_info[''])
            print(messages)

asyncio.get_event_loop().run_until_complete(client_main())
