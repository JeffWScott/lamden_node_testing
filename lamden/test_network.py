from lamden.sockets.request import Request, Result
from lamden.sockets.router import Router
from lamden.logger.base import get_logger

from lamden.crypto.wallet import Wallet
import os, json
import requests
import asyncio
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import zmq.asyncio


ACTION_PING = "ping"

logger = get_logger("MAIN")

router = None

async def run():
    print("RUNNING")
    while True:
        await asyncio.sleep(0)

def router_callback(ident_vk_string: str, msg: str) -> None:

    log = get_logger("ROUTER_CALLBACK")
    try:
        log.info(msg)
        msg = json.loads(msg)
        action = msg.get('action')
    except Exception as err:
        log.error(str(err))
        return

    if action == ACTION_PING:
        log.info(f"RESPONDING TO {msg.get('from')}")
        try:
            router.send_msg(
                to_vk="",
                msg_str=json.dumps({"response": "ping", "from": msg.get('from')})
            )
        except Exception as err:
            log.error(str(err))
            return

async def send_ping(request) -> dict:
    msg_obj = {'action': ACTION_PING, 'from': request.to_address}

    try:
        str_msg = json.dumps(msg_obj)
    except Exception as err:
        print(err)

        return None

    try:
        result = await request.send(str_msg=str_msg, timeout=1500, attempts=1)
        return handle_result(result=result)
    except Exception as error:
        print(error)

def handle_result(result: Result) -> (dict, None):
    if result.success:
        try:
            msg_json = result.response
            msg_json['success'] = result.success
            return msg_json

        except Exception as err:
            print(err)

    if result.error:
        print(result.error)

    return None

def request_address(ip: str) -> str:
    return 'tcp://{}:{}'.format(ip, 19000)

async def ping_everyone(node_ips: list, ctx):
    print("ping_everyone")
    peers = []
    for ip in node_ips:
        r = Request(to_address=request_address(ip=ip), ctx=ctx)
        r.start()
        peers.append(r)

    print("Sleeping before send")
    await asyncio.sleep(5)

    while True:
        for peer in peers:
            print(f'Sending ping to {peer.to_address}')
            await send_ping(peer)

        print("Done Sending, waiting a few seconds before going again...")
        await asyncio.sleep(20)

async def wait_for_start(router: Router):
    while not router.is_running:
        await asyncio.sleep(1)

async def get_ip():
    return requests.get('http://api.ipify.org').text

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    ctx = zmq.asyncio.Context()

    external_ip = requests.get('http://api.ipify.org').text
    external_address = '{}{}:{}'.format('tcp://', external_ip, 19000)

    print(external_address)

    sk = bytes.fromhex(os.environ['LAMDEN_SK'])
    node_ips = os.environ['OTHER_IPS'].split(':')
    print(node_ips)

    wallet = Wallet(seed=sk)

    router = Router(
        wallet=wallet,
        message_callback=router_callback,
        ctx=ctx,
        network_ip=external_address
    )
    router.set_address(port=19000)

    router.run_open_server()

    tasks = asyncio.gather(
        wait_for_start(router)
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks)

    tasks = asyncio.gather(
        ping_everyone(node_ips=node_ips, ctx=ctx)
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks)


