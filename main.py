
from datetime import datetime, timezone, timedelta
import traceback
from web3.main import Web3
from pprint import pprint
import time
import discord
from discord.ext import tasks
from utils import blockchain_urls, get_token_price_from_dexs
from discord.utils import get
import re
import sqlite3 as sl
import humanize
from discord.ext import commands
import aiohttp, asyncio
import urllib
import json
from subclasses import subclass_map
from eggs_utils import calc_pure_probability
from classes import classes_to_spacebarsize_map
from adfly import AdflyApi
import math
import concurrent
from asyncio import Semaphore
import uuid

SPAN = 100
SPAN_PAYMENTS=10000
SPAN_TIMESTAMP = 10
SNOWTRACE_SEM_ID = 0
APICRABADA_SEM_ID = 1
ADFLY_SEM_ID = 2
CRABALERT_SEM_ID = 3
previous_number_crabada = None
previous_number_payment = None
web3 = Web3(Web3.HTTPProvider(blockchain_urls["avalanche"]))
MEM_MAX = 15
wallet_pattern = "^0x[a-fA-F0-9]{40}$"
RATE = 1.3
TUS_CONTRACT_ADDRESS = "0xf693248F96Fe03422FEa95aC0aFbBBc4a8FdD172"
USDT_CONTRACT_ADDRESS = "0xc7198437980c041c805a1edcba50c1ce5db95118"
THRESOLD_PURE_PROBA = 0.8
ID_TUS_BOT = 910250184271867924
ID_SERVER = 932475138434277377
ID_COMMAND_CENTER = 933453311967887370
TIMEOUT = 2
SNOWTRACE_API_KEY = "KNDGDGKUAJ3UT1F8BHZDT61P2X453KGVAA"
NUMBER_BLOCKS_WAIT_BETWEEN_SNOWTRACE_CALLS = 5

stablecoins = {
    "0xc7198437980c041c805a1edcba50c1ce5db95118".lower()
}

coins = {
    "0xc7198437980c041c805a1edcba50c1ce5db95118".lower(): 6,
    TUS_CONTRACT_ADDRESS.lower(): 18
}

MINIMUM_PAYMENT = 0.5

historical_tus_price = []

channel_to_post_with_filters = {
    #Crabs and all
    932591668597776414: lambda x: True,
    934178951998357584: lambda x: True,
    933456755395006495: lambda x: True,
    935237809697095723: lambda x: True,
    933473949445144676: lambda x: x[1] is None and x[0].get("class_name", None) is not None,
    933456911913848912: lambda x: x[1] is None and x[0].get("pure_number", -1) is not None and x[0].get("pure_number", -1) == 6,
    933457087369978016: lambda x: x[1] is None and x[0].get("class_name", "") is not None and x[0].get("class_name", "").lower() == "prime",
    933399063330701414: lambda x: x[1] is None and x[0].get("breed_count", -1) is not None and x[0].get("breed_count", -1) == 0,
    933411792925913129: lambda x: x[1] is None and x[0].get("class_name", "") is not None and x[0].get("class_name", "").lower() == "craboid",
    933506031261188116: lambda x: (
        x[1] is None and
        x[0].get("breed_count", -1) is not None and
        x[0].get("pure_number", -1) is not None and
        x[0].get("breed_count", -1) == 0 and
        x[0].get("pure_number", -1) == 6
    ),
    933860819920355359: lambda x: x[1] is None and subclass_map.get(x[0].get("crabada_subclass", -1), "unknown")  == "Avalanche",
    933860865831223318: lambda x: x[1] is None and subclass_map.get(x[0].get("crabada_subclass", -1), "unknown")  == "Ethereum",
    933860950942044180: lambda x: x[1] is None and subclass_map.get(x[0].get("crabada_subclass", -1), "unknown")  == "Near",
    933861077312217129: lambda x: x[1] is None and subclass_map.get(x[0].get("crabada_subclass", -1), "unknown")  == "Fantom",
    933862594278740048: lambda x: x[1] is None and x[0].get("is_origin", -1) == 1,
    #Eggs
    933470546824396830: lambda x: x[1] is not None,
    933861312809799691: lambda x: x[1] is not None and x[1].get("probability_pure", 0) >= THRESOLD_PURE_PROBA,
    933861463414669422: lambda x: x[1] is not None and x[1].get("class_name_1", "") == "PRIME" and x[1].get("class_name_2", "") == "PRIME",
    934101749013291068: lambda x: x[1] is not None and (x[1].get("class_name_1", "") == "PRIME" or x[1].get("class_name_2", "") == "PRIME" and x[1].get("class_name_1", "") != x[1].get("class_name_2", "")),
    933861589738737664: lambda x: x[1] is not None and x[1].get("class_name_1", "") == "CRABOID" and x[1].get("class_name_2", "") == "CRABOID",
    934101847420055552: lambda x: x[1] is not None and (x[1].get("class_name_1", "") == "CRABOID" or x[1].get("class_name_2", "") == "CRABOID" and x[1].get("class_name_1", "") != x[1].get("class_name_2", "")),
}

cool_subclasses = {"near", "avalanche", "ethereum", "fantom"}

cool_classes = {"prime", "craboid"}

channels = {}

guild = None

async def refresh_prices_coin_except_tus():
    global coins
    set_price_coins({c: get_token_price_from_dexs(web3, "avalanche", c) for c in coins.keys() if c.lower() != TUS_CONTRACT_ADDRESS.lower()})


async def refresh_crabada_transactions(crabada_transactions):
    crabada_transactions_lst = [transaction for transaction in crabada_transactions if isinstance(transaction, dict) and is_valid_marketplace_transaction(transaction)]
    crabada_transactions = get_crabada_transactions()
    crabada_transactions += crabada_transactions_lst
    set_crabada_transactions(sorted(crabada_transactions, key=lambda x: int(x["blockNumber"])))
    if crabada_transactions != []:
        print(crabada_transactions)
        set_last_block_crabada_transaction(int(crabada_transactions[-1]["blockNumber"]) + 1)

def fetch_payments_coin_from_aux(wallet_address, from_timestamp, contract_address, r):
    decimals = coins.get(contract_address.lower(), 18)
    wallet_transactions = r
    wallet_transactions = [w for w in wallet_transactions if int(w["timeStamp"]) > int(from_timestamp)]
    rate = 1 if contract_address.lower() in stablecoins else 1.3
    price_coins = get_price_coins()
    price_in_usd = price_coins.get(contract_address.lower(), -1)
    return [(int(w["value"])*price_in_usd*10**-decimals)/rate for w in wallet_transactions if int(w["timeStamp"]) > int(from_timestamp) and w["from"].lower() == wallet_address.lower() and (int(w["value"])*10**-decimals*price_in_usd)/rate >= MINIMUM_PAYMENT]

async def fetch_payments_coin_from(wallet_address, from_timestamp, contract_address, previous_number):
    wallet_transactions_link = f"https://api.snowtrace.io/api?module=account&action=tokentx&contractaddress={contract_address}&address=0xbda6ffd736848267afc2bec469c8ee46f20bc342&startblock={previous_number}&sort=desc&endblock=999999999999&apikey={SNOWTRACE_API_KEY}"
    lst = await async_http_request_with_callback_on_result(
        wallet_transactions_link,
        SNOWTRACE_SEM_ID,
        lambda: [],
        TIMEOUT,
        lambda r: fetch_payments_coin_from_aux(wallet_address, from_timestamp, contract_address, r)
    )
    return lst

async def fetch_payments_from_aux(wallet_address, from_timestamp, r):
    price_coins = get_price_coins()
    tasks = [asyncio.create_task(fetch_payments_coin_from(wallet_address, from_timestamp, coin, int(r))) for coin in coins.keys() if price_coins.get(coin.lower(), -1) != -1]
    tasks = tuple(tasks)
    lst = await asyncio.gather(*tasks)
    return lst

async def fetch_payments_from(wallet_address, from_timestamp):
    global headers
    lst = await async_http_request_with_callback_on_result(
        f"https://api.snowtrace.io/api?module=block&action=getblocknobytime&timestamp={from_timestamp}&closest=before&apikey={SNOWTRACE_API_KEY}",
        SNOWTRACE_SEM_ID,
        lambda: [],
        TIMEOUT,
        lambda r: fetch_payments_from_aux(wallet_address, from_timestamp, r)
    )
    return lst
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://api.snowtrace.io/api?module=block&action=getblocknobytime&timestamp={from_timestamp}&closest=before&apikey={SNOWTRACE_API_KEY}", headers=headers) as r:
                if r.status == 200:
                    js = await r.json()
                    payments = asyncio.gather([fetch_payments_coin_from(wallet_address, from_timestamp, coin, int(js["result"])) for coin in coins.keys() if price_coins.get(coin.lower(), -1) != -1])
                    return sum(payments, [])
    except Exception as e:
        return []
    """


def open_database():
    try:
        connection = sl.connect('crabalert.db')
        return connection
    except Exception as e:
        #TODO : logging
        return None


def close_database(conn):
    conn.close()


def execute_query(conn, query):
    with conn:
        data = conn.execute(query)
        conn.commit()
    return list(data)

def is_valid_marketplace_transaction(transaction):
    return (
        "0x" + str(transaction.get("input", ""))[98:138].lower() == "0x7E8DEef5bb861cF158d8BdaAa1c31f7B49922F49".lower() and
        transaction["to"].lower() == "0x1b7966315ef0259de890f38f1bdb95acc03cacdd".lower()
    )

def get_channel(client, channel_id):
    global channels
    if channel_id not in channels:
        channel = client.get_channel(channel_id)
        if channel is not None:
            channels[channel_id] = client.get_channel(channel_id)
    return channels[channel_id]

def get_guild(client):
    global guild
    if guild is None:
        guild = client.get_guild(ID_SERVER)
    return guild

session = None

shared_vars = dict()

def get_price_coins():
    global shared_vars
    if "price_coins" not in shared_vars:
        shared_vars["price_coins"] = {
            coin: (-1 if coin.lower() not in stablecoins else 1) for coin in coins.keys()
        }
    return shared_vars["price_coins"]

def set_price_coins(value):
    global shared_vars
    shared_vars["price_coins"] = value

def get_price_tus():
    global shared_vars
    if "price_tus" not in shared_vars:
        shared_vars["price_tus"] = 0
    return shared_vars["price_tus"]

def get_block_number_ago():
    global shared_vars
    return shared_vars.get("block_number_ago", 0)

def set_block_number_ago(value):
    global shared_vars
    shared_vars["block_number_ago"] = value

def set_price_tus(value):
    global shared_vars
    shared_vars["price_tus"] = value

def get_already_seen():
    global shared_vars
    if "already_seen" not in shared_vars:
        shared_vars["already_seen"] = set()
    return shared_vars["already_seen"]

def set_already_seen(value):
    global shared_vars
    shared_vars["already_seen"] = value

def get_crabada_transactions():
    global shared_vars
    if "crabada_transactions" not in shared_vars:
        shared_vars["crabada_transactions"] = []
    return shared_vars["crabada_transactions"]

def set_crabada_transactions(value):
    global shared_vars
    shared_vars["crabada_transactions"] = value

def get_last_seen_block():
    global shared_vars
    return shared_vars.get("last_seen_block", 0)

def set_last_seen_block(value):
    global shared_vars
    shared_vars["last_seen_block"] = value

def get_last_block_crabada_transaction():
    global shared_vars
    return shared_vars.get("last_block_crabada_transaction", 0)

def set_last_block_crabada_transaction(value):
    global shared_vars
    shared_vars["last_block_crabada_transaction"] = value

def get_semaphore(id_semaphore, value=2):
    global shared_vars
    if "semaphores" not in shared_vars:
        shared_vars["semaphores"] = dict()
    if id_semaphore not in shared_vars["semaphores"]:
        shared_vars["semaphores"][id_semaphore] = asyncio.Semaphore(value)
    return shared_vars["semaphores"][id_semaphore]

headers={'User-Agent': 'Mozilla/5.0'}

channels_to_display_cashlinks = {
    934178951998357584
}

channels_emojis = {
    932591668597776414: {
        "tus": "<:tus:932645938944688148>",
        "crabadegg": "<:crabadegg:932909072653615144>",
        "crab1": "<:crab1:934087822254694441>",
        "crab2": "<:crab2:934087853732921384>"
    },
    935237809697095723: {
        "tus": "<:tus:935299315906256978>",
        "crabadegg": "<:crabadegg:935299133785387058>",
        "crab1": "<:crab_1:935299456658726933>",
        "crab2": "<:crab_2:935299577861509121>"
    },
    "default": {
        "tus": "<:tus:932797205767659521>",
        "crabadegg": "<:crabadegg:932809750624743454>",
        "crab1": "<:crab_1:934075767602700288>",
        "crab2": "<:crab_2:934076410132332624>"
    }
}

ADFLY_USER_ID = 26237719
ADFLY_PUBLIC_KEY = "3584d4107dc520de199b2a023b8c9a2b"
ADFLY_PRIVATE_KEY = "0b66ae48-4171-46c6-8c2a-2228358521bd"

api = AdflyApi(
    user_id=ADFLY_USER_ID,
    public_key=ADFLY_PUBLIC_KEY,
    secret_key=ADFLY_PRIVATE_KEY,
)

def is_crab(infos):
    return infos["class_name"] is not None


async def async_http_request_with_callback_on_result(url, url_id, callback_failure, timeout, f, *args, **kwargs):
    sem = get_semaphore(url_id)
    await sem.acquire()
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.get(url, headers=headers) as r:
                if r.status == 200:
                    js = await r.json()
                    task = asyncio.create_task(f(js["result"], *args, **kwargs))
                    await asyncio.sleep(2)
                    sem.release()
                    return task
    except Exception as e:
        # print"timeout", url)
        await asyncio.sleep(6)
        sem.release()
        return callback_failure()

async def notify_crab_item(infos_nft, token_id, price, timestamp_transaction, channel, cashlink=False):
    already_seen = get_already_seen()
    # print"crab from timestamp", timestamp_transaction,"will maybe be posted", token_id, "at channel", channel.id)
    if (token_id, timestamp_transaction, channel.id) not in already_seen:
        # print"crab from timestamp", timestamp_transaction,"will be posted", token_id, "at channel", channel.id)
        price_tus = get_price_tus()
        price_usd = round(price * price_tus, 2)
        channel_id = channel.id
        tus_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("tus", ":tus:")
        crabadegg_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crabadegg", "crabadegg")
        tus_text = f"{tus_emoji} **{price}**"
        tus_text_len_in_space_bars = sum([1 if c == "1" else 2 for c in str(price)]) + 6 + 1
        usd_text = f":moneybag: **{price_usd}**"
        purity_text = (':gem: **PURE**' if infos_nft['pure_number'] == 6 else ':diamond_shape_with_a_dot_inside: ' + str(infos_nft['pure_number']))
        purity_text_len_in_space_bars = 3 + 1 + (12 if infos_nft['pure_number'] == 6 else (1 if infos_nft['pure_number'] == 1 else 2))
        breed_text = f"{crabadegg_emoji} {infos_nft['breed_count'] if infos_nft['breed_count'] > 0 else '**BREED-FREE**'}"
        mining_text = f":pick: {infos_nft['speed'] + infos_nft['critical']}"
        mining_text_len_in_space_bars = 4 + 1 + sum([1 if c == "1" else 2 for c in str(infos_nft['speed'] + infos_nft['critical'])])

        battle_text = f":crossed_swords: {infos_nft['hp'] + infos_nft['armor'] + infos_nft['damage']}"

        max_len_space_text_first_column = max([tus_text_len_in_space_bars, purity_text_len_in_space_bars, mining_text_len_in_space_bars])

        first_column = tus_text + " "*((max_len_space_text_first_column*2 - tus_text_len_in_space_bars)) + usd_text
        second_column=purity_text + " "*((max_len_space_text_first_column*2 - purity_text_len_in_space_bars)) + breed_text
        third_column=mining_text + " "*((max_len_space_text_first_column*2 - mining_text_len_in_space_bars)) + battle_text

        subclass_display = subclass_map.get(infos_nft['crabada_subclass'], 'unknown')
        subclass_display = subclass_display if subclass_display.lower() not in cool_subclasses else f"**{subclass_display}**"
        class_display = infos_nft['class_name']
        class_display = class_display if class_display.lower() not in cool_classes else f"**{class_display}**"
        marketplace_link = f"https://marketplace.crabada.com/crabada/{token_id}"
        if cashlink:
            sem = get_semaphore(ADFLY_SEM_ID)
            await sem.acquire()
            shorten_link_dict = api.shorten(marketplace_link, domain="adf.ly", advert_type=1)
            marketplace_link = f"{shorten_link_dict['data'][0]['short_url']}"
            message = (
                f":crab: {class_display}({subclass_display})\n" +
                f"{first_column}\n" +
                marketplace_link
            )
            sem.release()
        else:
            message = (
                f":crab: {'**PURE**' if infos_nft['pure_number'] == 6 else ''}{' **ORIGIN**' if infos_nft['is_origin'] == 1 else ''}{' **BREED-FREE**' if infos_nft['breed_count'] == 0 else ''} {class_display}({subclass_display})\n" +
                f"{first_column}\n" +
                f"{second_column}\n" +
                f"{third_column}\n" +
                f"https://photos.crabada.com/{token_id}.png\n" +
                marketplace_link
            )
        if (token_id, timestamp_transaction, channel.id) not in already_seen:
            task = asyncio.create_task(channel.send(message))
            task.add_done_callback(create_already_seen_callback(token_id, timestamp_transaction, channel_id))

def create_already_seen_callback(token_id, t, channel_id):
    def callback(task):
        already_seen = get_already_seen()
        already_seen.add((token_id, t, channel_id))
        set_already_seen(already_seen)
    return callback

async def notify_egg_item(infos_family_nft, infos_nft, token_id, price, timestamp_transaction, channel, cashlink=False):
    filter_function = channel_to_post_with_filters[channel.id]
    already_seen = get_already_seen()
    # print"egg from timestamp", timestamp_transaction,"will maybe be posted", token_id, "at channel", channel.id)
    if (token_id, timestamp_transaction, channel.id) not in already_seen and filter_function((infos_nft, infos_family_nft)):
        # print"egg from timestamp", timestamp_transaction,"will be posted", token_id, "at channel", channel.id)
        channel_id = channel.id
        price_tus = get_price_tus()
        price_usd = round(price * price_tus, 2)
        infos_family_nft = infos_family_nft["crabada_parents"]
        crabada_parent_1 = infos_family_nft[0]
        crabada_parent_2 = infos_family_nft[1]
        class_parent_1 = crabada_parent_1["class_name"]
        class_parent_2 = crabada_parent_2["class_name"]
        dna_parent_1 = crabada_parent_1["dna"]
        dna_parent_2 = crabada_parent_2["dna"]
        egg_purity_probability = round(calc_pure_probability(dna_parent_1, dna_parent_2, class_parent_1), 2)
        tus_text = f"<tus> **{price}**"
        tus_text_len_in_space_bars = sum([1 if c == "1" else 2 for c in str(price)]) + 6 + 1
        usd_text = f":moneybag: **{price_usd}**"
        marketplace_link = f"https://marketplace.crabada.com/crabada/{token_id}"
        
        if class_parent_1 == class_parent_2:
            egg_class = class_parent_1
            egg_class_display = egg_class if egg_class not in cool_classes else f"**{egg_class}**"
            
            emoji_pure = ":gem:" if egg_purity_probability >= THRESOLD_PURE_PROBA else ":diamond_shape_with_a_dot_inside:"
            probability_display = f"**{int(egg_purity_probability*100)}%**" if egg_purity_probability >= THRESOLD_PURE_PROBA else f"{int(egg_purity_probability*100)}%"
            if egg_purity_probability == 1:
                probability_display = "**PURE**"
            egg_class_text = f":crab: {egg_class_display}"
            egg_class_text_len_in_space_bars = 4 + 1 + classes_to_spacebarsize_map.get(class_parent_1.upper(), 1)
            purity_probability_text = f"{emoji_pure} {probability_display}"
            
            max_len_space_text_first_column = max([tus_text_len_in_space_bars, egg_class_text_len_in_space_bars])
            first_column = tus_text + " "*((max_len_space_text_first_column*2 - tus_text_len_in_space_bars)) + usd_text
            second_column = f"{egg_class_text}" + " "*((max_len_space_text_first_column*2 - egg_class_text_len_in_space_bars)) + purity_probability_text
            message = (
                f"{first_column}\n" +
                f"{second_column}\n"
            )
            infos_egg = {
                "class_name_1": egg_class,
                "class_name_2": egg_class,
                "probability_pure": egg_purity_probability
            }
        
        else:
            egg_purity_probability_1 = egg_purity_probability
            egg_purity_probability_2 = round(calc_pure_probability(dna_parent_1, dna_parent_2, class_parent_1), 2)
            egg_class_1 = class_parent_1
            egg_class_2 = class_parent_2
            egg_class_display_1 = egg_class_1 if egg_class_1 not in cool_classes else f"**{egg_class_1}**"
            egg_class_display_2 = egg_class_2 if egg_class_2 not in cool_classes else f"**{egg_class_2}**"
            egg_class_display = f"({egg_class_display_1}|{egg_class_display_2})"
            egg_class_text_1 = f"<crab1> {egg_class_display_1}"
            egg_class_text_2 = f"<crab2> {egg_class_display_2}"
            egg_class_1_text_len_in_space_bars = 4 + 1 + classes_to_spacebarsize_map.get(class_parent_1.upper(), 1)
            emoji_pure_1 = ":gem:" if egg_purity_probability_1 >= THRESOLD_PURE_PROBA else ":diamond_shape_with_a_dot_inside:"
            emoji_pure_2 = ":gem:" if egg_purity_probability_2 >= THRESOLD_PURE_PROBA else ":diamond_shape_with_a_dot_inside:"
            probability_display_1 = f"**{int(egg_purity_probability_1*100)}%**" if egg_purity_probability_1 >= THRESOLD_PURE_PROBA else f"{int(egg_purity_probability_1*100)}%"
            probability_display_2 = f"**{int(egg_purity_probability_2*100)}%**" if egg_purity_probability_2 >= THRESOLD_PURE_PROBA else f"{int(egg_purity_probability_2*100)}%"
            purity_probability_text_1 = f"{emoji_pure_1} {probability_display_1}"
            purity_probability_text_1_len_in_space_bars = 4 + 1 + sum([1 if c == "1" or c == "." else 2 for c in str(int(egg_purity_probability_1*100))])
            purity_probability_text_2 = f"{emoji_pure_2} {probability_display_2}"

            max_len_space_text_first_column = max([tus_text_len_in_space_bars, egg_class_1_text_len_in_space_bars, purity_probability_text_1_len_in_space_bars])
            first_column = tus_text + " "*((max_len_space_text_first_column*2 - tus_text_len_in_space_bars)) + usd_text
            second_column = f"{egg_class_text_1}" + " "*((max_len_space_text_first_column*2 - egg_class_1_text_len_in_space_bars)) + f"{egg_class_text_2}"
            third_column = purity_probability_text_1 + " "*((max_len_space_text_first_column*2 - purity_probability_text_1_len_in_space_bars)) + purity_probability_text_2
            message = (
                f"{first_column}\n" +
                f"{second_column}\n" +
                f"{third_column}\n"
            )
            infos_egg = {
                "class_name_1": egg_class_1,
                "class_name_2": egg_class_2,
                "probability_pure": round(0.5*egg_purity_probability_1 + 0.5*egg_purity_probability_2, 2)
            }
        header_message = f"<crabadegg> {'**PURE** ' if infos_egg['probability_pure'] == 1.0 else ''}{egg_class_display} \n"
        footer_message = (
            f"https://i.ibb.co/hXcP49w/egg.png \n" +
            "<marketplace_link>"
        )
        crab_1_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crab1", ":crab1:")#"<:crab1:934087822254694441>" if channel_id == 932591668597776414 else "<:crab_1:934075767602700288>"
        crab_2_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crab2", ":crab2:")#"<:crab2:934087853732921384>" if channel_id == 932591668597776414 else "<:crab_2:934076410132332624>"
        tus_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("tus", ":tus:")
        crabadegg_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crabadegg", ":crabadegg:")
        if cashlink:
            sem = get_semaphore(ADFLY_SEM_ID)
            await sem.acquire()
            shorten_link_dict = api.shorten(marketplace_link, domain="adf.ly", advert_type=1)
            marketplace_link = f"{shorten_link_dict['data'][0]['short_url']}"
            message_egg = (
                f"{first_column}\n"
            )
            footer_message_egg = (
                "<marketplace_link>"
            )
            header_message_egg = (
                f"<crabadegg> {egg_class_display} \n"
            )
            sem.release()
        else:
            header_message_egg = header_message
            footer_message_egg = footer_message
            message_egg = message
        message_egg = header_message_egg + message_egg + footer_message_egg
        message_egg = message_egg.replace("<crab1>", crab_1_emoji).replace("<crab2>", crab_2_emoji).replace("<tus>", tus_emoji).replace("<crabadegg>" ,crabadegg_emoji).replace("<marketplace_link>", marketplace_link)
        if (token_id, timestamp_transaction, channel.id) not in already_seen:
            task = asyncio.create_task(channel.send(message_egg))
            task.add_done_callback(create_already_seen_callback(token_id, timestamp_transaction, channel_id))
        


async def handle_crabada_transaction(crabada_transaction):
    already_seen = get_already_seen()
    input_data = crabada_transaction["input"]
    token_id = int(input_data[138:202], 16)
    # print"token_id", token_id, "spotted !")
    price = int(round(int(input_data[330:], 16) * 10**-18, 0))
    link_nft_crabada = f"https://api.crabada.com/public/crabada/info/{token_id}"
    timestamp_transaction = int(crabada_transaction["timeStamp"])
    for channel_id in channel_to_post_with_filters.keys():
        channel = get_channel(client, channel_id)
        if channel is not None and (token_id, timestamp_transaction, channel_id) not in already_seen:
            asyncio.create_task(async_http_request_with_callback_on_result(link_nft_crabada, APICRABADA_SEM_ID, lambda: print("failure crabada transaction details"), TIMEOUT, notify_marketplace_item, token_id, price, channel_id, timestamp_transaction))

async def notify_marketplace_item(r, token_id, price, channel_id, timestamp_transaction):
    channel = get_channel(client, channel_id)
    filter_function = channel_to_post_with_filters[channel_id]
    if is_crab(r):
        if filter_function((r, None)):
            asyncio.create_task(notify_crab_item(r, token_id, price, timestamp_transaction, channel, cashlink=channel_id in channels_to_display_cashlinks))
    else:
        family_infos_link = f"https://api.crabada.com/public/crabada/family/{token_id}"
        asyncio.create_task(async_http_request_with_callback_on_result(
            family_infos_link, APICRABADA_SEM_ID, lambda: print("failure crabada egg transaction details"), TIMEOUT, notify_egg_item, r, token_id, price, timestamp_transaction, channel, cashlink=channel_id in channels_to_display_cashlinks
        ))
if __name__ == "__main__":
    
    intents = discord.Intents().all()
    intents.reactions = True
    intents.members = True
    intents.guilds = True
    
    client = commands.Bot(command_prefix="!", intents=intents)
    dt = datetime.now(timezone.utc)
    utc_time = dt.replace(tzinfo=timezone.utc)
    current_timestamp_ago = int(round(utc_time.timestamp(), 0)) - SPAN_TIMESTAMP

    #Look for last block up to one day ago
    req = urllib.request.Request(f"https://api.snowtrace.io/api?module=block&action=getblocknobytime&timestamp={current_timestamp_ago}&closest=before&apikey={SNOWTRACE_API_KEY}", headers=headers)
    block_number_ago = int(json.loads(urllib.request.urlopen(req).read())["result"])

    # Get last block for crabada transaction
    req = urllib.request.Request(
        f"https://api.snowtrace.io/api?module=account&action=txlist&address=0x1b7966315eF0259de890F38f1bDB95Acc03caCdD&startblock={block_number_ago}&sort=desc&endblock=999999999999&apikey={SNOWTRACE_API_KEY}",
        headers=headers
    )
    set_block_number_ago(block_number_ago)
    set_last_block_crabada_transaction(max([block_number_ago]+[int(transaction["blockNumber"]) for transaction in json.loads(urllib.request.urlopen(req).read())["result"] if isinstance(transaction, dict) and is_valid_marketplace_transaction(transaction)]))
    set_last_seen_block(block_number_ago)
    time.sleep(5)


    @client.event
    async def on_ready():
        channel = get_channel(client, ID_COMMAND_CENTER)
        await channel.send("Hi ! I'm back.")
        refresh_crabada_transactions_loop.start()
        crabada_alert_loop.start()
        refresh_tus_loop.start()
        manage_alerted_roles.start()
        refresh_prices_coin_except_tus_loop.start()
    @tasks.loop(seconds=1)
    async def crabada_alert_loop():
        sem = get_semaphore(CRABALERT_SEM_ID, value=1)
        await sem.acquire()
        set_transaction_id = {transaction["hash"] for transaction in get_crabada_transactions()}
        crabada_transactions = get_crabada_transactions()
        set_crabada_transactions([transaction for transaction in get_crabada_transactions() if transaction["hash"] not in set_transaction_id])
        sem.release()
        for transactions_base in crabada_transactions:
            set_transaction_id.add(transactions_base["hash"])
            asyncio.create_task(handle_crabada_transaction(transactions_base))
        
        """
        
            input_data = transactions_base["input"]
            token_id = int(input_data[138:202], 16)
            
            if token_id not in already_seen or transactions_base["timeStamp"] not in already_seen[token_id]:
                
                price = int(round(int(input_data[330:], 16) * 10**-18, 0))
                link_nft_crabada = f"https://api.crabada.com/public/crabada/info/{token_id}"
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(link_nft_crabada, headers=headers) as r:
                        if r.status == 200:
                            js = await r.json()
                            infos_nft = js["result"]
                            price_usd = round(price * price_tus, 2)
                            if infos_nft is not None:

                                if infos_nft["class_name"] is not None:
                                    if token_id not in already_seen:
                                        already_seen[token_id] = []
                                    already_seen[token_id].append(transactions_base["timeStamp"])
                                    for channel_id, filter_function in channel_to_post_with_filters.items():
                                        
                                        notify = filter_function((infos_nft, None))
                                        if notify:
                                            channel = get_channel(client, channel_id)
                                            if channel is not None:
                                                tus_emoji = "<:tus:932645938944688148>" if channel_id == 932591668597776414 else "<:tus:932797205767659521>"
                                                crabadegg_emoji = "<:crabadegg:932809750624743454>" if channel_id == 932591668597776414 else "<:crabadegg:932809750624743454>"
                                                tus_text = f"{tus_emoji} **{price}**"
                                                tus_text_len_in_space_bars = sum([1 if c == "1" else 2 for c in str(price)]) + 6 + 1
                                                usd_text = f":moneybag: **{price_usd}**"
                                                purity_text = (':gem: **PURE**' if infos_nft['pure_number'] == 6 else ':diamond_shape_with_a_dot_inside: ' + str(infos_nft['pure_number']))
                                                purity_text_len_in_space_bars = 3 + 1 + (12 if infos_nft['pure_number'] == 6 else (1 if infos_nft['pure_number'] == 1 else 2))
                                                breed_text = f"{crabadegg_emoji} {infos_nft['breed_count'] if infos_nft['breed_count'] > 0 else '**BREED-FREE**'}"
                                                mining_text = f":pick: {infos_nft['speed'] + infos_nft['critical']}"
                                                mining_text_len_in_space_bars = 4 + 1 + sum([1 if c == "1" else 2 for c in str(infos_nft['speed'] + infos_nft['critical'])])

                                                battle_text = f":crossed_swords: {infos_nft['hp'] + infos_nft['armor'] + infos_nft['damage']}"

                                                max_len_space_text_first_column = max([tus_text_len_in_space_bars, purity_text_len_in_space_bars, mining_text_len_in_space_bars])

                                                first_column = tus_text + " "*((max_len_space_text_first_column*2 - tus_text_len_in_space_bars)) + usd_text
                                                second_column=purity_text + " "*((max_len_space_text_first_column*2 - purity_text_len_in_space_bars)) + breed_text
                                                third_column=mining_text + " "*((max_len_space_text_first_column*2 - mining_text_len_in_space_bars)) + battle_text

                                                subclass_display = subclass_map.get(infos_nft['crabada_subclass'], 'unknown')
                                                subclass_display = subclass_display if subclass_display.lower() not in cool_subclasses else f"**{subclass_display}**"
                                                class_display = infos_nft['class_name']
                                                class_display = class_display if class_display.lower() not in cool_classes else f"**{class_display}**"
                                                if channel_id == 934178951998357584:
                                                    shorten_link_dict = api.shorten(marketplace_link, domain="adf.ly", advert_type=1)
                                                    marketplace_link = f"{shorten_link_dict['data'][0]['short_url']}"
                                                    message = (
                                                        f":crab: {class_display}({subclass_display})\n" +
                                                        f"{first_column}\n" +
                                                        marketplace_link
                                                    )
                                                else:
                                                    marketplace_link = f"https://marketplace.crabada.com/crabada/{token_id}"
                                                    message = (
                                                        f":crab: {'**PURE**' if infos_nft['pure_number'] == 6 else ''}{' **ORIGIN**' if infos_nft['is_origin'] == 1 else ''}{' **BREED-FREE**' if infos_nft['breed_count'] == 0 else ''} {class_display}({subclass_display})\n" +
                                                        f"{first_column}\n" +
                                                        f"{second_column}\n" +
                                                        f"{third_column}\n" +
                                                        f"https://photos.crabada.com/{token_id}.png\n" +
                                                        marketplace_link
                                                    )
                                                await channel.send(message)
                                    #*await message_to_edit.edit(content=message.replace(f"\nhttps://photos.crabada.com/{token_id}.png", ""))
                                else:
                                    family_infos_link = f"https://api.crabada.com/public/crabada/family/{token_id}"
                                    async with aiohttp.ClientSession() as session:
                                        async with session.get(family_infos_link, headers=headers) as r2:
                                            if r2.status == 200:
                                                if token_id not in already_seen:
                                                    already_seen[token_id] = []
                                                already_seen[token_id].append(transactions_base["timeStamp"])
                                                js = await r2.json()
                                                infos_family_nft = js["result"]["crabada_parents"]
                                                
                                                crabada_parent_1 = infos_family_nft[0]
                                                crabada_parent_2 = infos_family_nft[1]
                                                class_parent_1 = crabada_parent_1["class_name"]
                                                class_parent_2 = crabada_parent_2["class_name"]
                                                dna_parent_1 = crabada_parent_1["dna"]
                                                dna_parent_2 = crabada_parent_2["dna"]
                                                egg_purity_probability = round(calc_pure_probability(dna_parent_1, dna_parent_2, class_parent_1), 2)
                                                tus_text = f"<tus> **{price}**"
                                                tus_text_len_in_space_bars = sum([1 if c == "1" else 2 for c in str(price)]) + 6 + 1
                                                usd_text = f":moneybag: **{price_usd}**"
                                                marketplace_link = f"https://marketplace.crabada.com/crabada/{token_id}"
                                                
                                                if class_parent_1 == class_parent_2:
                                                    egg_class = class_parent_1
                                                    egg_class_display = egg_class if egg_class not in cool_classes else f"**{egg_class}**"
                                                    
                                                    emoji_pure = ":gem:" if egg_purity_probability >= THRESOLD_PURE_PROBA else ":diamond_shape_with_a_dot_inside:"
                                                    probability_display = f"**{int(egg_purity_probability*100)}%**" if egg_purity_probability >= THRESOLD_PURE_PROBA else f"{int(egg_purity_probability*100)}%"
                                                    if egg_purity_probability == 1:
                                                        probability_display = "**PURE**"
                                                    egg_class_text = f":crab: {egg_class_display}"
                                                    egg_class_text_len_in_space_bars = 4 + 1 + classes_to_spacebarsize_map.get(class_parent_1.upper(), 1)
                                                    purity_probability_text = f"{emoji_pure} {probability_display}"
                                                    
                                                    max_len_space_text_first_column = max([tus_text_len_in_space_bars, egg_class_text_len_in_space_bars])
                                                    first_column = tus_text + " "*((max_len_space_text_first_column*2 - tus_text_len_in_space_bars)) + usd_text
                                                    second_column = f"{egg_class_text}" + " "*((max_len_space_text_first_column*2 - egg_class_text_len_in_space_bars)) + purity_probability_text
                                                    message = (
                                                        f"{first_column}\n" +
                                                        f"{second_column}\n"
                                                    )
                                                    infos_egg = {
                                                        "class_name_1": egg_class,
                                                        "class_name_2": egg_class,
                                                        "probability_pure": egg_purity_probability
                                                    }
                                                
                                                else:
                                                    egg_purity_probability_1 = egg_purity_probability
                                                    egg_purity_probability_2 = round(calc_pure_probability(dna_parent_1, dna_parent_2, class_parent_1), 2)
                                                    egg_class_1 = class_parent_1
                                                    egg_class_2 = class_parent_2
                                                    egg_class_display_1 = egg_class_1 if egg_class_1 not in cool_classes else f"**{egg_class_1}**"
                                                    egg_class_display_2 = egg_class_2 if egg_class_2 not in cool_classes else f"**{egg_class_2}**"
                                                    egg_class_display = f"({egg_class_display_1}|{egg_class_display_2})"
                                                    egg_class_text_1 = f"<crab1> {egg_class_display_1}"
                                                    egg_class_text_2 = f"<crab2> {egg_class_display_2}"
                                                    egg_class_1_text_len_in_space_bars = 4 + 1 + classes_to_spacebarsize_map.get(class_parent_1.upper(), 1)
                                                    emoji_pure_1 = ":gem:" if egg_purity_probability_1 >= THRESOLD_PURE_PROBA else ":diamond_shape_with_a_dot_inside:"
                                                    emoji_pure_2 = ":gem:" if egg_purity_probability_2 >= THRESOLD_PURE_PROBA else ":diamond_shape_with_a_dot_inside:"
                                                    probability_display_1 = f"**{int(egg_purity_probability_1*100)}%**" if egg_purity_probability_1 >= THRESOLD_PURE_PROBA else f"{int(egg_purity_probability_1*100)}%"
                                                    probability_display_2 = f"**{int(egg_purity_probability_2*100)}%**" if egg_purity_probability_2 >= THRESOLD_PURE_PROBA else f"{int(egg_purity_probability_2*100)}%"
                                                    purity_probability_text_1 = f"{emoji_pure_1} {probability_display_1}"
                                                    purity_probability_text_1_len_in_space_bars = 4 + 1 + sum([1 if c == "1" or c == "." else 2 for c in str(int(egg_purity_probability_1*100))])
                                                    purity_probability_text_2 = f"{emoji_pure_2} {probability_display_2}"

                                                    max_len_space_text_first_column = max([tus_text_len_in_space_bars, egg_class_1_text_len_in_space_bars, purity_probability_text_1_len_in_space_bars])
                                                    first_column = tus_text + " "*((max_len_space_text_first_column*2 - tus_text_len_in_space_bars)) + usd_text
                                                    second_column = f"{egg_class_text_1}" + " "*((max_len_space_text_first_column*2 - egg_class_1_text_len_in_space_bars)) + f"{egg_class_text_2}"
                                                    third_column = purity_probability_text_1 + " "*((max_len_space_text_first_column*2 - purity_probability_text_1_len_in_space_bars)) + purity_probability_text_2
                                                    message = (
                                                        f"{first_column}\n" +
                                                        f"{second_column}\n" +
                                                        f"{third_column}\n"
                                                    )
                                                    infos_egg = {
                                                        "class_name_1": egg_class_1,
                                                        "class_name_2": egg_class_2,
                                                        "probability_pure": round(0.5*egg_purity_probability_1 + 0.5*egg_purity_probability_2, 2)
                                                    }
                                                header_message = f"<crabadegg> {'**PURE** ' if infos_egg['probability_pure'] == 1.0 else ''}{egg_class_display} \n"
                                                footer_message = (
                                                    f"https://i.ibb.co/hXcP49w/egg.png \n" +
                                                    "<marketplace_link>"
                                                )
                                                for channel_id, filter_function in channel_to_post_with_filters.items():
                                                    notify = filter_function((infos_nft, infos_egg))
                                                    if notify:
                                                        channel = get_channel(client, channel_id)
                                                        if channel is not None:
                                                            crab_1_emoji = "<:crab1:934087822254694441>" if channel_id == 932591668597776414 else "<:crab_1:934075767602700288>"
                                                            crab_2_emoji = "<:crab2:934087853732921384>" if channel_id == 932591668597776414 else "<:crab_2:934076410132332624>"
                                                            tus_emoji = "<:tus:932645938944688148>" if channel_id == 932591668597776414 else "<:tus:932797205767659521>"
                                                            crabadegg_emoji = "<:crabadegg:932909072653615144>" if channel_id == 932591668597776414 else "<:crabadegg:932809750624743454>"
                                                            if channel_id == 934178951998357584:
                                                                shorten_link_dict = api.shorten(marketplace_link, domain="adf.ly", advert_type=1)
                                                                marketplace_link = f"{shorten_link_dict['data'][0]['short_url']}"
                                                                message_egg = (
                                                                    f"{first_column}\n"
                                                                )
                                                                footer_message_egg = (
                                                                    "<marketplace_link>"
                                                                )
                                                                header_message_egg = (
                                                                    f"<crabadegg> {egg_class_display} \n"
                                                                )
                                                            else:
                                                                header_message_egg = header_message
                                                                footer_message_egg = footer_message
                                                                message_egg = message
                                                            message_egg = header_message_egg + message_egg + footer_message_egg
                                                            message_egg = message_egg.replace("<crab1>", crab_1_emoji).replace("<crab2>", crab_2_emoji).replace("<tus>", tus_emoji).replace("<crabadegg>" ,crabadegg_emoji).replace("<marketplace_link>", marketplace_link)
                                                            await channel.send(message_egg)
        """


    @tasks.loop(minutes=1)
    async def refresh_tus_loop():
        await client.wait_until_ready()
        server = get_guild(client)
        tus_bot = await server.fetch_member(ID_TUS_BOT)
        set_price_tus(float(tus_bot.nick.split(" ")[0][1:]))
        dt = datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc)
        current_timestamp = utc_time.timestamp()
        historical_tus_price.append((current_timestamp, get_price_tus()))
        

    @tasks.loop(minutes=1)
    async def refresh_prices_coin_except_tus_loop():
        await refresh_prices_coin_except_tus()
        

    @tasks.loop(seconds=1)
    async def refresh_crabada_transactions_loop():
        #await refresh_crabada_transactions()
        global SPAN
        current_block = web3.eth.block_number
        last_block_crabada_transaction = get_last_block_crabada_transaction()
        last_block_seen = get_last_seen_block()
        if current_block - last_block_seen >= NUMBER_BLOCKS_WAIT_BETWEEN_SNOWTRACE_CALLS:
            set_last_seen_block(current_block)
            last_block_crabada_transaction = get_last_block_crabada_transaction()
            link_transactions = f"https://api.snowtrace.io/api?module=account&action=txlist&address=0x1b7966315eF0259de890F38f1bDB95Acc03caCdD&startblock={last_block_crabada_transaction}&sort=desc&apikey={SNOWTRACE_API_KEY}"
            asyncio.create_task(async_http_request_with_callback_on_result(link_transactions, SNOWTRACE_SEM_ID, lambda: set_last_seen_block(last_block_seen), TIMEOUT, refresh_crabada_transactions))
        

    @tasks.loop(seconds=1)
    async def manage_alerted_roles():
        await client.wait_until_ready()
        channel = get_channel(client, 933456755395006495)
        if channel is None:
            return
        guild = get_guild(client)
        role_alerted = get(guild.roles, name="Alerted")
        try:
            connection = open_database()
        except Exception as e:
            #TODO : logging
            return
        async for member in channel.guild.fetch_members(limit=None):
            roles_str = [str(role) for role in member.roles]
            if not "Admin" in roles_str and not "Moderator" in roles_str and ("Verified" in roles_str or "Alerted" in roles_str):
                # Two cases :
                # Member is Alerted. Database is fetched to check that payment is up to date.
                # If member not in database, role is immediately revoked and we continue with others, otherwise...
                # If current_timestamp - payment_timestamp_txhash is lower than the duration, we leave it as it is.
                # (DM is sent to the user 1 week as a reminder)
                # Otherwise, we check all payments coming from this wallet in the explorer, occured after that timestamp and up to now.
                # All these payments will add their ratio in terms of USDT per second, and subscription starts from the first of these payments
                # 
                # Member is not Alerted. If not in database, skip.
                # If in database, we check all payments coming from this wallet in the explorer, occured after that timestamp and up to now.
                # If payment_timestamp is 0 then it is needed to check all the txs.
                # All these payments will add their ratio in terms of USDT per second, and subscription starts from the first of these payments

                dt = datetime.now(timezone.utc)
                utc_time = dt.replace(tzinfo=timezone.utc)
                current_timestamp = utc_time.timestamp()
                current_timestamp_datetime = datetime.fromtimestamp(current_timestamp, timezone.utc)
                data = list(execute_query(
                    connection, f"SELECT * FROM trial_day WHERE discord_id = '{member.id}'",
                ))
                rowcount = len(data)
                if rowcount > 0:
                    start_trial, duration_trial = data[0][1:]
                    if current_timestamp - datetime.strptime(start_trial, "%Y-%m-%d %H:%M:%S").astimezone(timezone.utc).timestamp() <= duration_trial:
                        if "Alerted" not in roles_str:
                            await member.add_roles(role_alerted)
                        continue
                    else:
                        if "Alerted" in roles_str:
                            await member.remove_roles(role_alerted)


                data = list(execute_query(
                    connection, f"SELECT * FROM last_received_payment WHERE discord_id = '{member.id}'",
                ))
                rowcount = len(data)
                if "Alerted" in roles_str and rowcount == 0:
                    await member.remove_roles(role_alerted)
                if rowcount > 0:
                    wallet_address, payment_date, duration, txhash, reminded = data[0][1:]
                    if duration < 0:
                        if "Alerted" not in roles_str:
                            await member.add_roles(role_alerted)
                    else:
                        reminded = reminded.lower() == "true"
                        if payment_date == "0000-00-00 00:00:00":
                            payment_timestamp = 0
                        else:
                            payment_timestamp = int(round(datetime.strptime(payment_date, "%Y-%m-%d %H:%M:%S").astimezone(timezone.utc).timestamp(), 0))
                        payments = await fetch_payments_from(wallet_address, payment_timestamp)
                        payments = sum(payments, [])
                        if current_timestamp - int(payment_timestamp) > duration:
                            if payments == [] and "Alerted" in roles_str:
                                await member.remove_roles(role_alerted)
                                await member.send("Your subscription to Crabalert has expired and your access to the alerts removed.")
                                await member.send("If you want to renew it, please send payments again (see #instructions for a soft reminder)")
                        else:
                            #Send reminders if subscription is close (< 1h)
                            remaining_time = (int(payment_timestamp) + int(duration)) - current_timestamp
                            if remaining_time <= 3600*24 and not reminded:
                                remaining_deltatime = humanize.naturaltime(current_timestamp + timedelta(seconds = remaining_time))
                                await member.send(f"Your subscription to Crabalert is going to expire in less than one day (remaining:{remaining_deltatime}). If you want to keep it, please send payment again (see #instructions for a soft reminder)")
                                update_query = f"UPDATE last_received_payment SET reminded='TRUE' WHERE discord_id='{member.id}'"
                                execute_query(connection, update_query)
                            if "Alerted" not in roles_str:
                                await member.add_roles(role_alerted)
                        if payments != []:
                            new_duration = (sum(payments)/10) * 3600 * 24 * 30 + max((int(payment_timestamp) + int(duration)) - current_timestamp, 0)
                            new_received_timestamp = datetime.fromtimestamp(current_timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                            update_query = f"UPDATE last_received_payment SET duration={int(round(new_duration, 0))}, received_timestamp='{new_received_timestamp}', reminded='FALSE' WHERE discord_id='{member.id}'"
                            execute_query(connection, update_query)
                            if "Alerted" not in roles_str:
                                await member.add_roles(role_alerted)
                            delta_duration = timedelta(seconds = new_duration + 3600*24*30)
                            human_friendly_duration = humanize.naturaldelta(current_timestamp_datetime - (current_timestamp_datetime+delta_duration), when=current_timestamp_datetime)
                            await member.send(f"Your payment has been checked and you have now access to alerts for a duration of {human_friendly_duration} starting from now.")
        close_database(connection)

    @client.command(name="rw", brief="Register public wallet address for subscription (see #instructions)")
    async def register(ctx, wallet: str = ""):
        guild = get_guild(client)
        member = await guild.fetch_member(ctx.author.id)
        roles_str = [str(role) for role in member.roles]
        if "Verified" not in roles_str or "Admin" in roles_str or "Moderator" in roles_str:
            if "Verified" not in roles_str:
                await ctx.channel.send(f'Please verify yourself to Crabalert before registering (go to https://discord.gg/PxyXk4TT).')
            return
        if re.match(wallet_pattern, wallet):
            discord_id = ctx.author.id
            connection = open_database()
            data = execute_query(
                connection, f"SELECT discord_id, from_wallet FROM last_received_payment WHERE discord_id = '{discord_id}'",
            )
            # gets the number of rows affected by the command executed
            rowcount = len(data)
            if rowcount == 0:
                try:
                    insert_wallet = f"INSERT INTO last_received_payment (discord_id, from_wallet, received_timestamp, txn_hash, reminded) VALUES('{discord_id}', '{wallet}', 0, '', 'FALSE')"
                    status = execute_query(connection, insert_wallet)
                    if status == 1:
                        await ctx.channel.send(f'Your wallet {wallet} has been added in the database. Next step, send to 0xbda6ffd736848267afc2bec469c8ee46f20bc342 10 USDTs times the number of months(1 month = 30 days) you want to suscribe (for example 3 months = 30 USDT, minimum 1 month).')
                        await ctx.channel.send(f'Once payment is received, you will get access to the alerts for the duration you have suscribed month based on your payment, since the payment reception datetime.')
                        await ctx.channel.send(f'If you don\'t have access to the alerts after 1 hour (Transaction are checked every 10 minutes in explorer), please open a ticket and provide your wallet address')
                except Exception as e:
                    await ctx.channel.send(f'Something went wrong. Please open a ticket and send the following error message : {str(e)}.')
            else:
                if wallet.lower() == data[0][1]:
                    await ctx.channel.send(f'Your wallet has already been associated with your Discord ID in our database.')
                else:
                    await ctx.channel.send(f'A wallet ({data[0][1]}) has already been added to our database. If you wish to change it, please open a ticket and provide your new wallet address.')
            close_database(connection)

    @client.command(name="shutdown", brief="Shutdown the bot (reboot if script launched in an infinite bash loop, otherwise it is a shutdown)")
    async def reboot(ctx):
        guild = get_guild(client)
        member = await guild.fetch_member(ctx.author.id)
        roles_str = [str(role) for role in member.roles]
        if "Admin" not in roles_str and "Moderator" not in roles_str:
            print("You cannot execute that command.")
            return
        await ctx.channel.send(f'Good bye.')
        exit(1)
        
    
    client.run('OTMyNDc4NjQ1ODE2MTQzOTA5.YeTkaQ.AzMetNL0LwuPYh7NOFrZvhG4VzQ')
    print("destroyed")
