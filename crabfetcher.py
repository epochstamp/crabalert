import argparse
import asyncio
from calendar import c
from datetime import datetime, timezone
from distutils.log import info
from requests import HTTPError, ReadTimeout
from web3.middleware import geth_poa_middleware
from web3.exceptions import TransactionNotFound
import memcache
from urllib3.exceptions import ReadTimeoutError
from is_origin import is_origin
from is_genesis import is_genesis
from config import (
    APICRABADA_SEM_ID,
    COINS,
    MAX_TIMEOUT_BEFORE_KILL_SNOWTRACE,
    MAX_TIMEOUT_BEFORE_KILL_CRABADAPI,
    NUMBER_BLOCKS_WAIT_BETWEEN_SNOWTRACE_CALLS,
    PAYMENT_SEM_ID,
    SNOWTRACE_SEM_ID,
    SPAN_TIMESTAMP,
    HEADERS,
    SNOWTRACE_API_KEY,
    TIMEOUT,
    LISTING_ITEM_EXPIRATION,
    TUS_CONTRACT_ADDRESS,
    WALLET_PATTERN,
    stablecoins,
    subclass_map,
    subclass_type_map
)
from utils import (
    blockchain_urls,
    calculate_token_price,
    close_database,
    execute_query,
    get_token_price_from_dexs,
    get_transactions_between_blocks,
    get_transactions_between_blocks_async,
    is_crab,
    async_http_get_request_with_callback_on_result_v2,
    get_current_block,
    open_contract,
    open_database
)
from web3 import Web3
import json

"""
https://subnet-test-api.crabada.com/public/order/matched/117699
To find history of transactions. Might be useful...
"""

marketplace_link_per_blockchain = {
    "avalanche": "https://marketplace.crabada.com/crabada",
    "swimmer_test": "https://marketplace-subnet-test.crabada.com/crabada",
    #Not definitive marketplace link, placeholder...
    "swimmer": "https://market.crabada.com/crabada"
}

photos_link_per_blockchain = {
    "avalanche": "https://photos.crabada.com",
    "swimmer_test": "https://swimmer-testnet-photos.crabada.com",
    "swimmer": "https://swimmer-photos.crabada.com"
}

explorer_link_per_blockchain = {
    "avalanche": "https://snowtrace.io/address",
    "swimmer_test": "https://testnet-explorer.swimmer.network/address",
    "swimmer": "https://explorer.swimmer.network/address"
}

crabada_contracts = {
    "avalanche": {
        "marketplace_contract": ("0x7E8DEef5bb861cF158d8BdaAa1c31f7B49922F49", "marketplace_abi.json"),
        "crabada_contract": ("0x1b7966315eF0259de890F38f1bDB95Acc03caCdD", "crabada_abi.json")
    },
    "swimmer_test": {
        "marketplace_contract": ("0x90a7E700e37437cCB693C4579f72bbD50bC17bc7", "marketplace_swimmer_abi.json"),
        "crabada_contract": ("0xe56cb40A104cf2783EA912859B4Ca7dE77cdC961", "crabada_swimmer_abi.json"),
        "crabdata_contract": ("0x5b4E7C3508e01e9386d0d7dda2519DF18fe5953A", "crabdata_swimmer_abi.json")
    },
    "swimmer": {
        "marketplace_contract": ("0x134e84507b0FBc5136C2217B847BbbF4c1A074B1", "marketplace_swimmer_abi.json"),
        "crabada_contract": ("0x620FF3d705EDBc1bd03e17E6afcaC36a9779f78D", "crabada_swimmer_abi.json"),
        "crabdata_contract": ("0xE0Ba7f212C1DBb04Db1604b0616783F0dda0523a", "crabdata_swimmer_abi.json")
    }
}

nftinfo_apis = {
    "avalanche": "https://api.crabada.com/public/crabada/info",
    "swimmer_test": "https://subnet-test-api.crabada.com/public/crabada/info",
    "swimmer": "https://market-api.crabada.com/public/crabada/info",
}

def is_valid_marketplace_listing_transaction(transaction, blockchain="swimmer_test"):
    return (
        "0x" + transaction.input[98:138].lower() == crabada_contracts[blockchain]["marketplace_contract"][0].lower() and
        transaction["to"].lower() == crabada_contracts[blockchain]["crabada_contract"][0].lower()
    )

def is_valid_marketplace_selling_transaction(transaction, blockchain="avalanche"):
    return (
        str(transaction.get("input", ""))[:10].lower() == "0xc70f5eaa".lower() and
        transaction["to"].lower() == crabada_contracts[blockchain]["marketplace_contract"][0].lower()
    )

class Aliasstr:

    def __init__(self, s):
        self._s = s
    
    def substring(self, a, b):
        return self._s[a: b]


def dna2hex(dna):
    dec = int(dna)
    return hex(dec).split('x')[-1]


def lookup_class(value: int) -> str:
        if value == 0:
            return "UNKNOWN"
        elif (value <= 8):
            return 'SURGE'
        elif (value < 24):
            return 'SUNKEN'
        elif (value < 39):
            return 'PRIME'
        elif (value < 54):
            return 'BULK'
        elif (value < 69):
            return 'CRABOID'
        elif (value < 84):
            return 'RUINED'
        elif (value < 99):
            return 'GEM'
        elif (value < 114):
            return 'ORGANIC'


def lookup_subclass(n):
    return subclass_map.get(n, "unknown")


def lookup_subclass_type(n):
    return subclass_type_map.get(n, "unknown")


BASES_STATS = {
    "SURGE":
    {
        "HP": 128,
        "ATTACK": 44,
        "ARMOR": 28,
        "SPEED": 22,
        "CRITICAL": 33,
    },
    "BULK":
    {
        "HP": 125,
        "ATTACK": 47,
        "ARMOR": 26,
        "SPEED": 23,
        "CRITICAL": 34,
    },
    "PRIME":
    {
        "HP": 108,
        "ATTACK": 58,
        "ARMOR": 22,
        "SPEED": 25,
        "CRITICAL": 42,
    },
    "GEM":
    {
        "HP": 121,
        "ATTACK": 48,
        "ARMOR": 27,
        "SPEED": 24,
        "CRITICAL": 35,
    },
    "SUNKEN":
    {
        "HP": 114,
        "ATTACK": 50,
        "ARMOR": 25,
        "SPEED": 29,
        "CRITICAL": 37,
    },
    "CRABOID":
    {
        "HP": 109,
        "ATTACK": 56,
        "ARMOR": 23,
        "SPEED": 27,
        "CRITICAL": 40,
    },
    "RUINED":
    {
        "HP": 110,
        "ATTACK": 54,
        "ARMOR": 24,
        "SPEED": 26,
        "CRITICAL": 41,
    },
    "ORGANIC":
    {
        "HP": 120,
        "ATTACK": 48,
        "ARMOR": 24,
        "SPEED": 29,
        "CRITICAL": 34,
    }
}

COMP_MAPPING_CHAR = {
    "horn_0": "HP",
    "pincer_0": "ATTACK",
    "shell_0": "ARMOR",
    "body_0": "SPEED",
    "mouth_0": "CRITICAL"
}

COMP_L_MAPPING_CHAR = {
    "HP": "lhorn",
    "ATTACK": "lpincer",
    "ARMOR": "lshell",
    "SPEED": "lbody",
    "CRITICAL": "lmouth"
}

API_MAPPING_CHAR = {
    "HP": "hp",
    "ATTACK": "damage",
    "ARMOR": "armor",
    "SPEED": "speed",
    "CRITICAL": "critical"
}

COMP_I_MAPPING_CHAR = {
    "HP": "horn_0",
    "ATTACK": "pincer_0",
    "ARMOR": "shell_0",
    "SPEED": "body_0",
    "CRITICAL": "mouth_0"
}

COMP_STATS = {
    "SURGE":
    {
        "HP": 3,
        "ATTACK": 0,
        "ARMOR": 5,
        "SPEED": 1,
        "CRITICAL": 1,
    },
    "BULK":
    {
        "HP": 4,
        "ATTACK": 0,
        "ARMOR": 5,
        "SPEED": 1,
        "CRITICAL": 0,
    },
    "PRIME":
    {
        "HP": 1,
        "ATTACK": 4,
        "ARMOR": 0,
        "SPEED": 1,
        "CRITICAL": 4,
    },
    "GEM":
    {
        "HP": 5,
        "ATTACK": 1,
        "ARMOR": 4,
        "SPEED": 0,
        "CRITICAL": 0,
    },
    "SUNKEN":
    {
        "HP": 3,
        "ATTACK": 2,
        "ARMOR": 1,
        "SPEED": 4,
        "CRITICAL": 0,
    },
    "CRABOID":
    {
        "HP": 2,
        "ATTACK": 3,
        "ARMOR": 0,
        "SPEED": 0,
        "CRITICAL": 5,
    },
    "RUINED":
    {
        "HP": 2,
        "ATTACK": 5,
        "ARMOR": 0,
        "SPEED": 0,
        "CRITICAL": 3,
    },
    "ORGANIC":
    {
        "HP": 2,
        "ATTACK": 2,
        "ARMOR": 2,
        "SPEED": 2,
        "CRITICAL": 2,
    }
}

BONUS_PURITY = {**{i: i*0.02 for i in range(0, 6)}, **{6: 0.15}}
BONUS_LEGEND_DIFF_CLASS = 0.05
BONUS_LEGEND_SAME_CLASS = 0.1

def info_from_dna(dna):
    hexString = dna2hex(dna)
    dnafixed = Aliasstr(f"0{hexString}")

    subclass_name = lookup_subclass(int(dnafixed.substring(2, 4), 16))
    class_name = lookup_class(int(dnafixed.substring(2, 4), 16))
    subclass_type = lookup_subclass_type(int(dnafixed.substring(2, 4), 16))

    legend_shell = lookup_class(int(dnafixed.substring(4, 6), 16))
    legend_horn = lookup_class(int(dnafixed.substring(6, 8), 16))
    legend_body = lookup_class(int(dnafixed.substring(8, 10), 16))
    legend_mouth = lookup_class(int(dnafixed.substring(10, 12), 16))
    legend_pincer = lookup_class(int(dnafixed.substring(12, 14), 16))

    shellr0 = lookup_subclass(int(dnafixed.substring(28, 30), 16))
    shellr0_class = lookup_class(int(dnafixed.substring(28, 30), 16))
    shellr1 = lookup_subclass(int(dnafixed.substring(30, 32), 16))
    shellr2 = lookup_subclass(int(dnafixed.substring(32, 34), 16))

    hornr0 = lookup_subclass(int(dnafixed.substring(34, 36), 16))
    hornr0_class = lookup_class(int(dnafixed.substring(34, 36), 16))
    hornr1 = lookup_subclass(int(dnafixed.substring(36, 38), 16))
    hornr2 = lookup_subclass(int(dnafixed.substring(38, 40), 16))

    bodyr0 = lookup_subclass(int(dnafixed.substring(40, 42), 16))
    bodyr0_class = lookup_class(int(dnafixed.substring(40, 42), 16))
    bodyr1 = lookup_subclass(int(dnafixed.substring(42, 44), 16))
    bodyr2 = lookup_subclass(int(dnafixed.substring(44, 46), 16))

    mouthr0 = lookup_subclass(int(dnafixed.substring(46, 48), 16))
    mouthr0_class = lookup_class(int(dnafixed.substring(46, 48), 16))
    mouthr1 = lookup_subclass(int(dnafixed.substring(48, 50), 16))
    mouthr2 = lookup_subclass(int(dnafixed.substring(50, 52), 16))

    eyer0 = lookup_subclass(int(dnafixed.substring(52, 54), 16))
    eyer0_class = lookup_class(int(dnafixed.substring(52, 54), 16))
    eyer1 = lookup_subclass(int(dnafixed.substring(54, 56), 16))
    eyer2 = lookup_subclass(int(dnafixed.substring(56, 58), 16))

    pincerr0 = lookup_subclass(int(dnafixed.substring(58, 60), 16))
    pincerr0_class = lookup_class(int(dnafixed.substring(58, 60), 16))
    pincerr1 = lookup_subclass(int(dnafixed.substring(60, 62), 16))
    pincerr2 = lookup_subclass(int(dnafixed.substring(62, 64), 16))

    purity = 0
    for component in [shellr0_class, hornr0_class, bodyr0_class, mouthr0_class, eyer0_class, pincerr0_class]:
        if component == class_name:
            purity += 1

    breeding_match = 0
    for component in [shellr0, shellr1, shellr2, hornr0, hornr1, hornr2, bodyr0, bodyr1, bodyr2, mouthr0, mouthr1, mouthr2, eyer0, eyer1, eyer2, pincerr0, pincerr1, pincerr2]:
        if component == subclass_name:
            breeding_match += 1
    legend_parts = {"lshell": legend_shell, "lhorn": legend_horn, "lbody": legend_body, "lmouth": legend_mouth, "lpincer": legend_pincer}
    base_info = {
        "subclass": subclass_name,
        "class": class_name,
        "subclass_type": subclass_type,
        "shell_0": shellr0,
        "shell_1": shellr1,
        "shell_2": shellr2,
        "horn_0": hornr0,
        "horn_1": hornr1,
        "horn_2": hornr2,
        "body_0": bodyr0,
        "body_1": bodyr1,
        "body_2": bodyr2,
        "mouth_0": mouthr0,
        "mouth_1": mouthr1,
        "mouth_2": mouthr2,
        "eye_0": eyer0,
        "eye_1": eyer1,
        "eye_2": eyer2,
        "pincer_0": pincerr0,
        "pincer_1": pincerr1,
        "pincer_2": pincerr2,
        "purity": purity,
        "breeding_match": breeding_match
    }
    comp_class_info = {"shell_0": shellr0_class, "horn_0": hornr0_class, "body_0": bodyr0_class, "mouth_0": mouthr0_class, "pincer_0": pincerr0_class}
    base_info = {**base_info, **legend_parts}

    crab_stats = dict(BASES_STATS[class_name])
    #print(crab_stats)
    for k, v in comp_class_info.items():
        crab_stats[COMP_MAPPING_CHAR[k]] += COMP_STATS[v][COMP_MAPPING_CHAR[k]]
    #print(crab_stats)
    n_same_legendary_class = len([lp for lp in legend_parts.values() if lp == class_name])
    n_diff_legendary_class = len([lp for lp in legend_parts.values() if lp.upper() != "UNKNOWN" and lp != class_name])
    k = "HP"
    #print(comp_class_info)
    #print(COMP_I_MAPPING_CHAR[k])
    #print(comp_class_info[COMP_I_MAPPING_CHAR[k]])
    #print((1+(BONUS_PURITY[purity]*(1 if comp_class_info[COMP_I_MAPPING_CHAR[k]] == class_name else 0))))
    crab_stats = {
        k: int(int(v*(1+(BONUS_PURITY[purity]*(1 if comp_class_info[COMP_I_MAPPING_CHAR[k]] == class_name else 0))))*(1 + n_same_legendary_class*BONUS_LEGEND_SAME_CLASS + n_diff_legendary_class*BONUS_LEGEND_DIFF_CLASS)) for k, v in crab_stats.items()
    }
    crab_stats["BP"] = crab_stats["HP"] + crab_stats["ATTACK"] + crab_stats["ARMOR"]
    crab_stats["MP"] = crab_stats["SPEED"] + crab_stats["CRITICAL"]
    base_info = {
        **base_info,
        **crab_stats
    }


    return base_info





class Crabfetcher:

    def __init__(self, blockchain="swimmer_test"):
        self._shared = memcache.Client(["127.0.0.1:11211"], debug=0)
        self._shared.set("nft_pool", dict())
        self._shared.set("order_id_pool", dict())
        self._blockchain = blockchain
        dt = datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc)
        #Look for last block up to one day ago
        self._web3 = Web3(Web3.HTTPProvider(blockchain_urls[self._blockchain], request_kwargs={'timeout': 4}))
        try:
            self._web3.middleware_onion.inject(geth_poa_middleware, layer=0)
        except:
            pass
        marketplace_contract_address, marketplace_contract_abi = crabada_contracts[self._blockchain]["marketplace_contract"]
        crabada_contract_address, crabada_contract_abi = crabada_contracts[self._blockchain]["crabada_contract"]
        self._crabdata_contract = None
        if self._blockchain != "avalanche":
            crabdata_contract_address, crabdata_contract_abi = crabada_contracts[self._blockchain]["crabdata_contract"]
            self._crabdata_contract = open_contract(self._web3, self._blockchain, crabdata_contract_address, json.load(open(crabdata_contract_abi)))
        self._marketplace_contract = open_contract(self._web3, self._blockchain, marketplace_contract_address, json.load(open(marketplace_contract_abi)))
        self._crabada_contract = open_contract(self._web3, self._blockchain, crabada_contract_address, json.load(open(crabada_contract_abi)))
        self._variables = {}

    @property
    def variables(self):
        return self._variables

    def _get_variable(self, name: str, f_value_if_not_exists=lambda: None):
        if name not in self._variables:
            self._variables[name] = f_value_if_not_exists()
        return self._variables[name]

    async def _set_variable(self, name: str, value):
        self._variables[name] = value

    def _set_sync_variable(self, name: str, value):
        self._variables[name] = value

    def _remove_variable(self, name: str):
        if name in self._variables.keys():
            self._variables.pop(name)
            
            
    """
    LISTING PART
    """ 
       


  
        

    async def _fetch_and_store_crabada_listing_entry(self, transactions):
        tasks = []
        for crabada_transaction in transactions:
            input_data = crabada_transaction["input"]
            token_id = int(input_data[138:202], 16)
            seller_wallet = crabada_transaction["from"]
            selling_price = int(input_data[330:], 16)
            timestamp_transaction = int(crabada_transaction["timeStamp"])
            nft_pool = self._shared.get("nft_pool")
            order_id, = self._call_contract_function(self._marketplace_contract.functions.currentOrderId())
            last_order_id = order_id
            if (token_id, timestamp_transaction, selling_price, False) not in nft_pool.keys():
                if order_id is not None:
                    seller_wallet_2, token_id_2, selling_price_2 = self._call_contract_function(self._marketplace_contract.functions.sellOrders(order_id), n_values=3)
                    while (seller_wallet_2 != seller_wallet or token_id_2 != token_id or selling_price_2 != selling_price) and order_id > 0 and last_order_id - order_id <= 50:
                        order_id -= 1
                        seller_wallet_2, token_id_2, selling_price_2 = self._call_contract_function(self._marketplace_contract.functions.sellOrders(order_id), n_values=3)
                    if seller_wallet_2 == seller_wallet and token_id_2 == token_id and selling_price_2 == selling_price:
                        order_id_pool = self._shared.get("order_id_pool")
                        self._shared.set("order_id_pool", {**order_id_pool, **{order_id: (seller_wallet, None, token_id, selling_price, timestamp_transaction, None)}})
                    _, _, dna, _, _ = self._call_contract_function(self._crabada_contract.functions.crabadaInfo(token_id), n_values=5)
                    if dna is not None:
                        is_crab = dna != 0
                        link_nft_crabada = f"{nftinfo_apis[self._blockchain]}/{token_id}"
                        #_recall_crabada_infos_api_after_sleep(self, e, token_id, selling_price, timestamp_transaction, is_crab, order_id, seconds=2, is_selling=True, buyer_wallet=None, seller_wallet=None)
                        #_fetch_and_store_crabada_entry_aux(self, infos_nft, selling_price, token_id, timestamp, is_crab, order_id, is_selling=True, buyer_wallet=None, seller_wallet=None)
                        tasks.append(async_http_get_request_with_callback_on_result_v2(
                                link_nft_crabada,
                                self._recall_crabada_infos_api_after_sleep,
                                TIMEOUT,
                                self._fetch_and_store_crabada_entry_aux,
                                callback_failure_args=(token_id, selling_price, timestamp_transaction, is_crab, order_id),
                                callback_failure_kwargs={"is_selling": False, "seller_wallet": seller_wallet, "buyer_wallet": None},
                                f_args=(selling_price, token_id, timestamp_transaction, is_crab, order_id),
                                f_kwargs={"is_selling": False, "seller_wallet": seller_wallet},
                                semaphore=self._get_variable(f"sem_{APICRABADA_SEM_ID}", lambda: asyncio.Semaphore(value=1))
                            )
                        )
        if tasks != []:
            asyncio.gather(*tasks)

    """
    SELLING PART
    """
        

    async def _fetch_and_store_crabada_selling_entry(self, transactions):
        tasks = []
        for crabada_transaction in transactions:
            input_data = crabada_transaction["input"]
            order_id = int(input_data[10:], 16)
            order_id_pool = self._shared.get("order_id_pool")
            seller_wallet, _, token_id, selling_price, _, _ = order_id_pool.get(order_id, (None, None, None, None, None, None))
            seller_wallet = None
            if seller_wallet is None:
                try:
                    receipt = self._web3.eth.get_transaction_receipt(crabada_transaction["hash"])
                    if receipt["status"] == 1:
                        log = receipt["logs"][-1]
                        topic = log["topics"][-1]
                        seller_wallet = "0x" + str(topic[25:])
                        token_id = int(log["data"][:66], 16)
                        selling_price = int(log["data"][66:], 16)
                except TransactionNotFound as e:
                    pending_transactions = self._get_variable("pending_selling_transactions", lambda: [])
                    self._set_sync_variable("pending_selling_transactions", pending_transactions + [crabada_transaction])
                except Exception as e2:
                    print(e2)

            
            if seller_wallet is not None and token_id != 0:
                
                buyer_wallet = crabada_transaction["from"]
                timestamp_transaction = int(crabada_transaction["timeStamp"])
                nft_pool = self._shared.get("nft_pool")
                if (token_id, timestamp_transaction, selling_price, False) not in nft_pool.keys():
                    order_id_pool = self._shared.get("order_id_pool")
                    if order_id in order_id_pool.keys():
                        seller_wallet, _, token_id, selling_price, timestamp_listing, _ = order_id_pool[order_id]
                        self._shared.set("order_id_pool", {**order_id_pool, **{order_id: (seller_wallet, buyer_wallet, token_id, selling_price, timestamp_listing, timestamp_transaction)}})
                    _, _, dna, _, _ = self._call_contract_function(self._crabada_contract.functions.crabadaInfo(token_id), n_values=5)
                    if dna is not None:
                        is_crab = dna != 0
                        link_nft_crabada = f"{nftinfo_apis[self._blockchain]}/{token_id}"
                        tasks.append(async_http_get_request_with_callback_on_result_v2(
                                link_nft_crabada,
                                self._recall_crabada_infos_api_after_sleep,
                                TIMEOUT,
                                self._fetch_and_store_crabada_entry_aux,
                                callback_failure_args=(token_id, selling_price, timestamp_transaction, is_crab, order_id),
                                callback_failure_kwargs={"is_selling": True, "buyer_wallet": buyer_wallet, "seller_wallet": seller_wallet},
                                f_args=(selling_price, token_id, timestamp_transaction, is_crab, order_id),
                                f_kwargs={"is_selling": True, "buyer_wallet": buyer_wallet, "seller_wallet": seller_wallet},
                                semaphore=self._get_variable(f"sem_{APICRABADA_SEM_ID}", lambda: asyncio.Semaphore(value=1))
                            )
                        )
        if tasks != []:
            asyncio.gather(*tasks)
    """
    COMMON
    """

    def _call_contract_function(self, contract_function, n_values=1, caching=False, caching_id=""):
        #TODO : cache !
        try:
            if caching:
                cache_by_caching_id = self._get_variable(f"caching_keys_{caching_id}", lambda: set())
                out = self._get_variable(f"{contract_function.fn_name}_{str(contract_function.args)}_{caching_id}_out", lambda: contract_function.call())
                if out is None:
                    out = contract_function.call()
                self._set_sync_variable(f"caching_keys_{caching_id}", cache_by_caching_id.union({f"{contract_function.fn_name}_{str(contract_function.args)}_{caching_id}_out"}))
                self._set_sync_variable(f"{contract_function.fn_name}_{str(contract_function.args)}_{caching_id}_out", out)
            else:
                out = contract_function.call()
            if not isinstance(out, list):
                out = (out,)
            else:
                out = tuple(out)
            return out
        except (HTTPError, ReadTimeout) as e:
            print(f'[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}]', "READING CONTRACT ERROR:", contract_function.fn_name, contract_function.args, ":", type(e), e)
            return (None,) * n_values
        except ConnectionError:
            exit(1)

    def _fetch_and_store_crabada_infos_by_smart_contract(self, token_id, dna, crab1, crab2, is_crab, breeding_count, is_selling=True, buyer_wallet=None, seller_wallet=None):
        _, _, dna1, _, _ = self._call_contract_function(self._crabada_contract.functions.crabadaInfo(crab1), n_values=5)
        _, _, dna2, _, _ = self._call_contract_function(self._crabada_contract.functions.crabadaInfo(crab2), n_values=5)
        infos_nft = None
        if dna1 is not None and dna2 is not None:
            infos_nft = dict()
            infos_nft["owner_full_name"] = None
            if is_selling:
                infos_nft["owner"] = buyer_wallet
            else:
                infos_nft["owner"] = seller_wallet
            if is_crab:
                crabada_infos = info_from_dna(dna)
                infos_nft = {**infos_nft, **crabada_infos}
                
                for crab_stats in ["HP", "ARMOR", "ATTACK", "SPEED", "CRITICAL"]:
                    infos_nft[API_MAPPING_CHAR[crab_stats]] = crabada_infos[crab_stats]
                infos_nft["id"] = token_id
                infos_nft["crabada_id"] = token_id
                infos_nft["dna"] = str(dna)
                infos_nft["crabada_subclass"] = [k for k,v in subclass_map.items() if v.lower() == crabada_infos["subclass"].lower()][0]
                infos_nft["pure_number"] = crabada_infos["purity"]
                infos_nft["breed_count"] = breeding_count
                infos_nft["class_name"] = crabada_infos["class"]
                infos_nft["is_genesis"] = (1 if token_id in is_genesis else 0)
                infos_nft["is_origin"] = (1 if token_id in is_origin else 0)


            
            else:
                crabada_1_infos = info_from_dna(dna1)
                crabada_2_infos = info_from_dna(dna2)
                parents_infos = [crabada_1_infos, crabada_2_infos]
                infos_nft["crabada_parents"] = parents_infos
                dnas = [dna1, dna2]
                for i in [0, 1]:
                    infos_nft["crabada_parents"][i]["class_name"] = parents_infos[i]["class"]
                    infos_nft["crabada_parents"][i]["dna"] = dnas[i]

        return infos_nft

    async def _recall_crabada_infos_api_after_sleep(self, e, token_id, selling_price, timestamp_transaction, is_crab, order_id, seconds=2, is_selling=True, buyer_wallet=None, seller_wallet=None):
        timeout_counter = self._get_variable(f"apicrabada_timeout_counter_{token_id}_{timestamp_transaction}_{is_selling}", lambda: 0)
        self._set_sync_variable(f"apicrabada_timeout_counter_{token_id}_{timestamp_transaction}_{is_selling}", timeout_counter + 1)
        if timeout_counter + 1 >= MAX_TIMEOUT_BEFORE_KILL_CRABADAPI:
            #Create the whole infos_nft dict using only smart contract
            self._remove_variable(f"apicrabada_timeout_counter_{token_id}_{timestamp_transaction}_{is_selling}")
            crab1, crab2, dna, _, breeding_count = self._call_contract_function(self._crabada_contract.functions.crabadaInfo(token_id), n_values=5)
            if dna is not None:
                infos_nft = self._fetch_and_store_crabada_infos_by_smart_contract(token_id, dna, crab1, crab2, is_crab, breeding_count, is_selling=True, buyer_wallet=buyer_wallet, seller_wallet=seller_wallet)
                if infos_nft is not None:
                    task = asyncio.create_task(self._fetch_and_store_crabada_entry_aux(
                        infos_nft,
                        selling_price,
                        token_id,
                        timestamp_transaction,
                        is_crab,
                        order_id,
                        is_selling=is_selling,
                        buyer_wallet=buyer_wallet,
                        seller_wallet=seller_wallet
                    ))
                else:
                    task = None
            else:
                task = asyncio.create_task(self._recall_crabada_infos_api_after_sleep(self, e, token_id, selling_price, timestamp_transaction, is_crab, order_id, seconds=seconds, is_selling=is_selling, buyer_wallet=buyer_wallet, seller_wallet=seller_wallet))

            if task is not None:
                asyncio.gather(task)
        else:
            await asyncio.sleep(seconds)
            link_nft_crabada = f"{nftinfo_apis[self._blockchain]}/{token_id}"
            nft_pool = self._shared.get("nft_pool")
            if (token_id, timestamp_transaction, selling_price, False) not in nft_pool.keys():
                task = async_http_get_request_with_callback_on_result_v2(
                                link_nft_crabada,
                                self._recall_crabada_infos_api_after_sleep,
                                TIMEOUT,
                                self._fetch_and_store_crabada_entry_aux,
                                callback_failure_args=(token_id, selling_price, timestamp_transaction, is_crab, order_id),
                                callback_failure_kwargs={"is_selling": is_selling, "seconds": seconds, "buyer_wallet": buyer_wallet, "seller_wallet": seller_wallet},
                                f_args=(selling_price, token_id, timestamp_transaction, is_crab, order_id),
                                f_kwargs={"is_selling": is_selling, "buyer_wallet": buyer_wallet, "seller_wallet": seller_wallet},
                                semaphore=self._get_variable(f"sem_{APICRABADA_SEM_ID}", lambda: asyncio.Semaphore(value=1))
                            )
                
                asyncio.gather(task)

    async def _fetch_and_store_crabada_transactions(self):
        current_block_transactions = self._web3.eth.get_block('latest', full_transactions=True)
        listing_transactions = [{**transaction, "timeStamp": current_block_transactions.timestamp} for transaction in current_block_transactions.transactions if is_valid_marketplace_listing_transaction(transaction, blockchain=self._blockchain)]
        selling_transactions = [{**transaction, "timeStamp": current_block_transactions.timestamp} for transaction in current_block_transactions.transactions if is_valid_marketplace_selling_transaction(transaction, blockchain=self._blockchain)]
        selling_transactions += self._get_variable("pending_selling_transactions", lambda: [])
        self._set_sync_variable("pending_selling_transactions", [])
        tasks = []
        if listing_transactions != []:
            tasks.append(asyncio.create_task(self._fetch_and_store_crabada_listing_entry(listing_transactions)))
        if selling_transactions != []:
            tasks.append(asyncio.create_task(self._fetch_and_store_crabada_selling_entry(selling_transactions)))
        if tasks != []:
            asyncio.gather(*tuple(tasks))

    async def _fetch_and_store_crabada_entry_aux(self, infos_nft, selling_price, token_id, timestamp, is_crab, order_id, is_selling=True, buyer_wallet=None, seller_wallet=None):
        type_entry = "selling" if is_selling else "listing"
        async with self._get_variable(f"last_block_crabada_{type_entry}_transaction_semaphore_{token_id}_{timestamp}_{selling_price}", lambda: asyncio.Semaphore(value=1)):
            nft_pool = self._shared.get("nft_pool")
            if (token_id, timestamp, selling_price, is_selling) not in nft_pool.keys():
                crab1, crab2, dna, _, breeding_count = self._call_contract_function(self._crabada_contract.functions.crabadaInfo(token_id), n_values=5)
                if dna is not None:
                    if not is_crab:
                        
                        _, _, dna1, _, _ = self._call_contract_function(self._crabada_contract.functions.crabadaInfo(crab1), n_values=5)
                        _, _, dna2, _, _ = self._call_contract_function(self._crabada_contract.functions.crabadaInfo(crab2), n_values=5)
                        if dna1 is not None and dna2 is not None and dna1 != 0 and dna2 != 0:
                            infos_crab_1 = info_from_dna(dna1)
                            infos_crab_2 = info_from_dna(dna2)
                            infos_nft["crabada_parents"] = [
                                {
                                    "class_name": infos_crab_1["class"],
                                    "dna": dna1
                                },
                                {
                                    "class_name": infos_crab_2["class"],
                                    "dna": dna2
                                }
                            ]
                        else:
                            infos_nft = None
                    else:
                        if infos_nft["pure_number"] is None:
                            infos_nft = self._fetch_and_store_crabada_infos_by_smart_contract(token_id, dna, crab1, crab2, is_crab, breeding_count, is_selling=is_selling, buyer_wallet=buyer_wallet, seller_wallet=seller_wallet)
                if infos_nft is not None:
                    infos_nft["price"] = selling_price * 10**-18
                    web3_avalanche = Web3(Web3.HTTPProvider(blockchain_urls["avalanche"], request_kwargs={'timeout': 15}))
                    try:
                        tus_price_in_avax = calculate_token_price(web3_avalanche, "avalanche", "0x565d20bd591b00ead0c927e4b6d7dd8a33b0b319", "0xf693248F96Fe03422FEa95aC0aFbBBc4a8FdD172")
                        avax_price_in_fiat = calculate_token_price(web3_avalanche, "avalanche", "0xf4003f4efbe8691b60249e6afbd307abe7758adb", "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7")
                    except ReadTimeoutError as _:
                        tus_price_in_avax = None
                        avax_price_in_fiat = None
                    if tus_price_in_avax is not None and avax_price_in_fiat is not None:
                        infos_nft["price_usd"] = tus_price_in_avax * infos_nft["price"] * avax_price_in_fiat
                        infos_nft["is_crab"] = is_crab
                        infos_nft["order_id"] = order_id
                        infos_nft["seller_wallet"] = seller_wallet
                        infos_nft["buyer_wallet"] = buyer_wallet
                        
                        if is_selling:
                            infos_nft["url_wallet"] = f"{explorer_link_per_blockchain[self._blockchain]}/{buyer_wallet}"
                            if infos_nft["owner_full_name"] is None:
                                infos_nft["owner_full_name"] = buyer_wallet
                        else:
                            infos_nft["url_wallet"] = f"{explorer_link_per_blockchain[self._blockchain]}/{seller_wallet}"
                            if infos_nft["owner_full_name"] is None:
                                infos_nft["owner_full_name"] = seller_wallet
                        infos_nft["marketplace_link"] = f"{marketplace_link_per_blockchain[self._blockchain]}/{token_id}"
                        if is_crab:
                            infos_nft["photos_link"] = f"{photos_link_per_blockchain[self._blockchain]}/{token_id}.png"
                        else:
                            infos_nft["photos_link"] = f"{photos_link_per_blockchain[self._blockchain]}/{token_id}.png"
                        import pprint
                        nft_pool = {**nft_pool, **{(token_id, timestamp, selling_price, is_selling): infos_nft}}
                        self._shared.set("nft_pool", nft_pool)

    async def _cleanup_shared_memory(self):
        #Cleanup nft pool
        nft_pool = self._shared.get("nft_pool")
        dt = datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc).timestamp()
        nft_pool = {
            k:v for k,v in nft_pool.items() if utc_time - k[1] <= 60
        }
        self._shared.set("nft_pool", nft_pool)
        #Cleanup order id pool
        order_id_pool = self._shared.get("order_id_pool")
        order_id_pool = {
            k:v for k,v in order_id_pool.items() if (v[-1] is not None and utc_time - v[-1] <= 60) or utc_time - v[-2] <= 3600*24*30
        }
        self._shared.set("order_id_pool", order_id_pool)
            
    async def run(self, seconds_transaction=1, seconds_cleanup_memory=60):
        counter_timer = 0
        while True:
            tasks = []
            if counter_timer % seconds_transaction == 0:
                tasks.append(self._fetch_and_store_crabada_transactions())
            if counter_timer % seconds_cleanup_memory == 0:
                tasks.append(self._cleanup_shared_memory())
            if tasks != []:
                asyncio.gather(*tuple(tasks))
            counter_timer = (counter_timer + 1) % 65
            await asyncio.sleep(1)
            

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Crab miner.')
    parser.add_argument('--blockchain', choices=["avalanche", "swimmer_test", "swimmer"],
                        help='Game blockchain', default="swimmer")
    args = parser.parse_args()
    asyncio.run(Crabfetcher(blockchain=args.blockchain).run())

