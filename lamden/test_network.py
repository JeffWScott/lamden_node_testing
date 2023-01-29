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

IP_MAP = {
   "134.122.98.27": "99339662e90b18249985bc377e4274ccc1de88a3d34a94f3f89a7a7fcad72680",
    "64.225.32.184": "c017239027e9880d35d90e19add32aefaf722bbfe5e0591b975d79e2be9c1295",
    "170.64.178.113": "c019700b432ef40a958be4e84b7d724f66b83951d84e3bff58e25eb14dba5583"

}

ACTION_PING = "ping"

logger = get_logger("MAIN")

router = None

async def run():
    print("RUNNING")
    while True:
        await asyncio.sleep(0)

async def router_callback(ident_vk_bytes: str, msg: str) -> None:

    log = get_logger("ROUTER_CALLBACK")
    try:
        log.info(msg)
        msg = json.loads(msg)
        action = msg.get('action')
    except Exception as err:
        log.error(str(err))
        return

    if action == ACTION_PING:
        #log.info(f"RESPONDING TO {msg.get('from')}")
        try:
            router.send_msg(
                ident_vk_bytes=ident_vk_bytes,
                msg_str=json.dumps({"response": "ping", "from": msg.get('from')}),
                ip=msg.get('from')
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
    logger.info(result)
    if result.success:
        try:
            msg_json = json.loads(result.response)
            msg_json['success'] = result.success
            return msg_json

        except Exception as err:
            logger.error(err)

    if result.error:
        logger.error(result.error)

    return None

def request_address(ip: str) -> str:
    return 'tcp://{}:{}'.format(ip, 19000)

async def ping_everyone(node_ips: list, ctx, wallet: Wallet):
    print("ping_everyone")
    peers = []
    for ip in node_ips:
        r = Request(
            to_address=request_address(ip=ip),
            ctx=ctx,
            server_curve_vk=IP_MAP[ip],
            local_wallet=wallet
        )
        r.start()
        peers.append(r)

    print("Sleeping before send")
    await asyncio.sleep(5)

    while True:
        for peer in peers:
            print(f'Sending ping to {peer.to_address}')
            res = await send_ping(peer)
            logger.info(res)

        print("Done Sending, waiting a few seconds before going again...")
        await asyncio.sleep(5)

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
    router.refresh_cred_provider_vks(vk_list=[
        "c017239027e9880d35d90e19add32aefaf722bbfe5e0591b975d79e2be9c1295",
        "c019700b432ef40a958be4e84b7d724f66b83951d84e3bff58e25eb14dba5583",
        "99339662e90b18249985bc377e4274ccc1de88a3d34a94f3f89a7a7fcad72680"
    ])

    #router.run_open_server()
    router.run_curve_server()

    tasks = asyncio.gather(
        wait_for_start(router)
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks)

    tasks = asyncio.gather(
        ping_everyone(node_ips=node_ips, ctx=ctx, wallet=wallet)
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks)


