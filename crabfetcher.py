import asyncio
from datetime import datetime, timezone
from config import (
    APICRABADA_SEM_ID,
    COINS,
    NUMBER_BLOCKS_WAIT_BETWEEN_SNOWTRACE_CALLS,
    PAYMENT_SEM_ID,
    SNOWTRACE_SEM_ID,
    SPAN_TIMESTAMP,
    HEADERS,
    SNOWTRACE_API_KEY,
    TIMEOUT,
    LISTING_ITEM_EXPIRATION,
    WALLET_PATTERN,
    stablecoins
)
from utils import (
    blockchain_urls,
    close_database,
    execute_query,
    get_transactions_between_blocks,
    get_transactions_between_blocks_async,
    is_crab,
    is_valid_marketplace_listing_transaction,
    iblock_near,
    is_valid_marketplace_selling_transaction,
    async_http_get_request_with_callback_on_result_v2,
    get_current_block,
    open_database
)
from web3 import Web3
import json
import urllib
import re

class Crabfetcher:

    def __init__(self):
        dt = datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc)
        current_timestamp_ago = int(round(utc_time.timestamp(), 0)) - SPAN_TIMESTAMP

        #Look for last block up to one day ago
        web3 = Web3(Web3.HTTPProvider(blockchain_urls["avalanche"]))
        try:
            req = urllib.request.Request(f"https://api.snowtrace.io/api?module=block&action=getblocknobytime&timestamp={current_timestamp_ago}&closest=before&apikey={SNOWTRACE_API_KEY}", headers=HEADERS)
            block_number_ago = int(json.loads(urllib.request.urlopen(req).read())["result"])
        except:
            block_number_ago = asyncio.run(iblock_near(web3, current_timestamp_ago))
        # Get last block for crabada transaction        
        try:
            req = urllib.request.Request(
                f"https://api.snowtrace.io/api?module=account&action=txlist&address=0x1b7966315eF0259de890F38f1bDB95Acc03caCdD&startblock={block_number_ago}&sort=desc&endblock=999999999999&apikey={SNOWTRACE_API_KEY}",
                headers=HEADERS
            )
            last_block_crabada_listing_transaction = max([block_number_ago]+[int(transaction["blockNumber"]) for transaction in json.loads(urllib.request.urlopen(req).read())["result"] if isinstance(transaction, dict) and is_valid_marketplace_listing_transaction(transaction)])
        except Exception as e:
            transactions = asyncio.run(get_transactions_between_blocks(web3, block_number_ago, filter_t=lambda t: is_valid_marketplace_listing_transaction(t)))
            last_block_crabada_listing_transaction = max([block_number_ago]+[int(transaction["blockNumber"]) for transaction in transactions if isinstance(transaction, dict) and is_valid_marketplace_listing_transaction(transaction)])
        try:
            req = urllib.request.Request(
                f"https://api.snowtrace.io/api?module=logs&action=getLogs&fromBlock={block_number_ago}&toBlock=99999999999&address=0x7e8deef5bb861cf158d8bdaaa1c31f7b49922f49&apikey={SNOWTRACE_API_KEY}&topic0=0x4d3b1cf93e7676f80b7df86edb68fdf7be63c9964cc44c6b43b51c434b8ab771",
                headers=HEADERS
            )
            last_block_crabada_selling_transaction = max([block_number_ago]+[int(transaction["blockNumber"], 16) for transaction in json.loads(urllib.request.urlopen(req).read())["result"] if isinstance(transaction, dict)])
        except Exception as e:
            transactions = asyncio.run(get_transactions_between_blocks(web3, block_number_ago, filter_t=lambda t: is_valid_marketplace_selling_transaction(t)))
            last_block_crabada_selling_transaction = max([block_number_ago]+[int(transaction["blockNumber"], 16) for transaction in transactions if isinstance(transaction, dict) and is_valid_marketplace_selling_transaction(transaction)])
        self._variables = {
            "block_number_ago": block_number_ago,
            "blocks_crabada_listing_transaction": {last_block_crabada_listing_transaction},
            "blocks_crabada_selling_transaction": {last_block_crabada_selling_transaction},
            "last_block_seen_selling": block_number_ago,
            "last_block_seen_listing": block_number_ago,
            "web3": web3
        }
        self._prepare_database()

    def _prepare_database(self):
        db = open_database()
        query = """
        CREATE TABLE IF NOT EXISTS crabada_sellings (
            token_id INT NOT NULL,
            selling_price INT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            is_crab BOOLEAN NOT NULL,
            infos_nft VARCHAR NOT NULL,
            infos_family VARCHAR,
            PRIMARY KEY (token_id, selling_price, timestamp)
        );
        """
        db.execute(query)
        db.commit()
        query = """
        CREATE TABLE IF NOT EXISTS 'crabada_listings' (
            token_id INT NOT NULL,
            selling_price INT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            is_crab BOOLEAN NOT NULL,
            infos_nft VARCHAR NOT NULL,
            infos_family VARCHAR,
            PRIMARY KEY (token_id, selling_price, timestamp)
        );
        """
        db.execute(query)
        db.commit()
        query = """
        CREATE TABLE IF NOT EXISTS 'last_received_payment' (
        discord_id char(66) NOT NULL,
        from_wallet char(42) NOT NULL,
        received_timestamp timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
        duration int(11) NOT NULL DEFAULT '0',
        txn_hash char(66) NOT NULL DEFAULT '0',
        reminded BIT NOT NULL DEFAULT 'FALSE',
        PRIMARY KEY (discord_id)
        );
        """
        db.execute(query)
        db.commit()
        query = """
        CREATE TABLE IF NOT EXISTS 'trials' (
        'discord_id' char(66) NOT NULL PRIMARY KEY,
        'start_trial' timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
        'duration_trial' int(11) NOT NULL DEFAULT '0',
        PRIMARY KEY (discord_id)
        )
        """
        db.execute(query)
        db.commit()
        query = """
        CREATE TABLE IF NOT EXISTS 'payments' (
            from_wallet char(42) NOT NULL,
            received_timestamp timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
            contract_address char(42) NOT NULL DEFAULT 0,
            txn_hash char(66) NOT NULL DEFAULT '0',
            value int(11) NOT NULL DEFAULT 0,
            PRIMARY KEY (from_wallet, received_timestamp, contract_address)
        )
        """
        db.execute(query)
        db.commit()

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

    """
    SUBSCRIPTION MANAGEMENT
    """
    async def _fetch_and_store_payments_loop(self, seconds=600):
        while True:
            await asyncio.sleep(seconds)
            task = asyncio.create_task(self._fetch_and_store_payments())
            asyncio.gather(task)
            

    async def _fetch_and_store_payments(self):
        dt = datetime.now(timezone.utc)
        utc_time = int(round(dt.replace(tzinfo=timezone.utc).timestamp(), 0))
        payment_timestamp = self._get_variable("payment_timestamp", lambda: int(round(utc_time, 0)) - 3600*24*365)
        task = async_http_get_request_with_callback_on_result_v2(
                f"https://api.snowtrace.io/api?module=block&action=getblocknobytime&timestamp={payment_timestamp}&closest=before&apikey={SNOWTRACE_API_KEY}",
                self._fetch_and_store_payments_web3,
                TIMEOUT,
                self._fetch_and_store_payments_aux,
                f_args = (payment_timestamp,),
                callback_failure_args=(payment_timestamp,),
                semaphore=self._get_variable(f"sem_{SNOWTRACE_SEM_ID}", lambda: asyncio.Semaphore(value=1))
            )
        
        asyncio.gather(task)

    async def _fetch_and_store_payments_web3(self, e, payment_timestamp):
        web3 = Web3(Web3.HTTPProvider(blockchain_urls["avalanche"]))
        task = asyncio.create_task(iblock_near(web3, payment_timestamp))
        task.add_done_callback(lambda t: asyncio.create_task(self._fetch_and_store_payments_aux(t.result(), payment_timestamp)))
        asyncio.gather(task)

    async def _fetch_and_store_payments_aux(self, block_number, payment_timestamp):
        tasks_parameters = []
        for contract_address in COINS.keys():
            #wallet_transactions_link = f"https://api.snowtrace.io/api?module=account&action=tokentx&contractaddress={contract_address}&address=0xbda6ffd736848267afc2bec469c8ee46f20bc342&startblock={block_number}&sort=desc&endblock=999999999999&apikey={SNOWTRACE_API_KEY}"
            tasks_parameters.append((payment_timestamp, contract_address, block_number))
            
        if tasks_parameters != []:
            asyncio.gather(*(
                async_http_get_request_with_callback_on_result_v2(
                    f"https://api.snowtrace.io/api?module=account&action=tokentx&contractaddress={contract_address}&address=0xbda6ffd736848267afc2bec469c8ee46f20bc342&startblock={block_number}&sort=desc&endblock=999999999999&apikey={SNOWTRACE_API_KEY}",
                    self._fetch_and_store_payments_web3_2,
                    TIMEOUT,
                    self._store_payment_aux,
                    f_args = (payment_timestamp, contract_address),
                    callback_failure_args=(block_number, payment_timestamp, contract_address),
                    semaphore=self._get_variable(f"sem_{SNOWTRACE_SEM_ID}", lambda: asyncio.Semaphore(value=1))
                ) for payment_timestamp, contract_address, block_number in tasks_parameters
            ))

    async def _fetch_and_store_payments_web3_2(self, e, block_number, payment_timestamp, contract_address):
        task = asyncio.create_task(get_transactions_between_blocks_async(
                lambda: Web3(Web3.HTTPProvider(blockchain_urls["avalanche"])),
                block_number,
                filter_t = lambda t: (
                    t["to"].lower() == contract_address.lower() and
                    t["input"][34:74].lower() == "0xbda6ffd736848267afc2bec469c8ee46f20bc342".lower() and
                    int(t["timeStamp"]) > payment_timestamp
                ),
                callback = lambda r: self._store_payment_aux(r, payment_timestamp, contract_address)
            )
        )
        asyncio.gather(task)

    async def _store_payment_aux(self, transactions, payment_timestamp, contract_address):
        if not isinstance(transactions, list):
            transactions = [transactions]
        decimals = COINS.get(contract_address.lower(), 18)
        async with self._get_variable(f"sem_{PAYMENT_SEM_ID}", lambda: asyncio.Semaphore(value=1)):
            wallet_transactions = transactions
            try:
                wallet_transactions = [{**{"value": int(w["input"][74:], 16)} ,**w} for w in wallet_transactions if int(w["timeStamp"]) > int(payment_timestamp)]
            except Exception as e:
                wallet_transactions = [w for w in wallet_transactions if int(w["timeStamp"]) > int(payment_timestamp)]
            lst = [((int(w["value"])*(10**-decimals)), int(w["timeStamp"]), w["hash"], w["from"].lower()) for w in wallet_transactions]
            db = open_database()
            if lst != []:
                timestamp_max = lst[0][1]
                for value, timestamp, hash, from_wallet in lst:
                    execute_query(
                        db,
                        f"""
                            INSERT OR REPLACE INTO payments (from_wallet, received_timestamp, contract_address, txn_hash, value)
                            VALUES('{from_wallet}', {timestamp}, '{contract_address}', '{hash}', {value})
                        """
                    )
                    timestamp_max = max(timestamp_max, timestamp)
            self._set_sync_variable("payment_timestamp", timestamp_max)
            close_database(db)
            
    """
    LISTING PART
    """ 

    async def _clean_crabada_listing_database_loop(self, seconds=60):
        while True:
            await asyncio.sleep(seconds)
            task = asyncio.create_task(self._clean_crabada_listing_database())
            asyncio.gather(task)
            

    async def _clean_crabada_listing_database(self):
        async with self._get_variable("sem_database_listing", lambda: asyncio.Semaphore(value=1)):
            db = open_database()
            dt = datetime.now(timezone.utc)
            utc_time = int(round(dt.replace(tzinfo=timezone.utc).timestamp(), 0))
            query = f"""
            DELETE FROM crabada_listings WHERE {utc_time} - timestamp >= {LISTING_ITEM_EXPIRATION};
            """
            execute_query(
                db,
                query
            )
            close_database(db)


    
            
    
    async def _fetch_and_store_crabada_listing_transactions(self):
        web3 = Web3(Web3.HTTPProvider(blockchain_urls["avalanche"]))
        task = asyncio.create_task(
            get_current_block(web3)
        )
        task.add_done_callback(
            lambda t: asyncio.create_task(self._fetch_and_store_crabada_listing_transactions_aux(
                t.result()
            ))
        )
        asyncio.gather(task)

    async def _fetch_and_store_crabada_listing_transactions_aux(self, current_block):
        last_block_crabada_listing_transaction = max(self._get_variable("blocks_crabada_listing_transaction", lambda: {0}))
        link_transactions = f"https://api.snowtrace.io/api?module=account&action=txlist&address=0x1b7966315eF0259de890F38f1bDB95Acc03caCdD&startblock={last_block_crabada_listing_transaction}&sort=desc&apikey={SNOWTRACE_API_KEY}"
        last_block_seen = self._get_variable("last_block_seen_listing", lambda: 0)
        if current_block - last_block_seen >= NUMBER_BLOCKS_WAIT_BETWEEN_SNOWTRACE_CALLS:
            self._set_sync_variable("last_block_seen_listing", current_block)
            task = async_http_get_request_with_callback_on_result_v2(
                    link_transactions,
                    self._fetch_and_store_crabada_listing_transactions_web3,
                    TIMEOUT,
                    self._fetch_and_store_crabada_listing_entry,
                    callback_failure_args = (current_block, last_block_crabada_listing_transaction),
                    semaphore=self._get_variable(f"sem_{SNOWTRACE_SEM_ID}", lambda: asyncio.Semaphore(value=1))
                )
            asyncio.gather(task)

    async def _fetch_and_store_crabada_listing_transactions_web3(self, e, current_block, last_block_crabada_transaction):
        task = asyncio.create_task(
            get_transactions_between_blocks_async(lambda: Web3(Web3.HTTPProvider(blockchain_urls["avalanche"])), last_block_crabada_transaction, end_block=current_block, filter_t=lambda t: is_valid_marketplace_listing_transaction(t), convert_to_logs=False, callback = self._fetch_and_store_crabada_listing_entry)
        )
        asyncio.gather(task)

    async def _recall_crabada_api_after_sleep(self, e, token_id, block_number, selling_price, timestamp_transaction, seconds=2, is_selling=True):
        await asyncio.sleep(seconds)
        link_nft_crabada = f"https://api.crabada.com/public/crabada/info/{token_id}"
        already_seen = self._get_variable("already_seen", lambda: set())
        if (token_id, timestamp_transaction, selling_price, is_selling) not in already_seen:
            task = async_http_get_request_with_callback_on_result_v2(
                            link_nft_crabada,
                            self._recall_crabada_api_after_sleep,
                            TIMEOUT,
                            self._fetch_and_store_crabada_entry_aux,
                            callback_failure_args=(token_id, block_number, selling_price, timestamp_transaction),
                            callback_failure_kwargs={"is_selling": False, "seconds": seconds},
                            f_args=(block_number, selling_price, token_id, timestamp_transaction),
                            f_kwargs={"is_selling": False},
                            semaphore=self._get_variable(f"sem_{APICRABADA_SEM_ID}", lambda: asyncio.Semaphore(value=1))
                        )
            
            asyncio.gather(task)

    async def _fetch_and_store_crabada_listing_entry(self, transactions):
        tasks = []
        for crabada_transaction in transactions:
            if isinstance(crabada_transaction, dict) and is_valid_marketplace_listing_transaction(crabada_transaction):
                block_number = int(crabada_transaction["blockNumber"])
                input_data = crabada_transaction["input"]
                token_id = int(input_data[138:202], 16)
                
                selling_price = int(round(int(input_data[330:], 16) * 10**-18, 0))
                link_nft_crabada = f"https://api.crabada.com/public/crabada/info/{token_id}"
                timestamp_transaction = int(crabada_transaction["timeStamp"])
                already_seen = self._get_variable("already_seen", lambda: set())
                if (token_id, timestamp_transaction, selling_price, False) not in already_seen:
                    print(f"spotted listing item {token_id}")
                    tasks.append(async_http_get_request_with_callback_on_result_v2(
                            link_nft_crabada,
                            self._recall_crabada_api_after_sleep,
                            TIMEOUT,
                            self._fetch_and_store_crabada_entry_aux,
                            callback_failure_args=(token_id, block_number, selling_price, timestamp_transaction),
                            callback_failure_kwargs={"is_selling": False},
                            f_args=(block_number, selling_price, token_id, timestamp_transaction),
                            f_kwargs={"is_selling": False},
                            semaphore=self._get_variable(f"sem_{APICRABADA_SEM_ID}", lambda: asyncio.Semaphore(value=1))
                        )
                    )
        if tasks != []:
            asyncio.gather(*tasks)

    """
    SELLING PART
    """

    async def _clean_crabada_selling_database_loop(self, seconds=60):
        while True:
            await asyncio.sleep(seconds)
            task = asyncio.create_task(self._clean_crabada_selling_database())
            asyncio.gather(task)
            

    async def _clean_crabada_selling_database(self):
        async with self._get_variable("sem_database_selling", lambda: asyncio.Semaphore(value=1)):
            db = open_database()
            dt = datetime.now(timezone.utc)
            utc_time = int(round(dt.replace(tzinfo=timezone.utc).timestamp(), 0))
            query = f"""
            DELETE FROM crabada_sellings WHERE {utc_time} - timestamp >= {LISTING_ITEM_EXPIRATION};
            """
            execute_query(
                db,
                query
            )
            close_database(db)
            

    async def _fetch_and_store_crabada_selling_transactions(self):
        web3 = Web3(Web3.HTTPProvider(blockchain_urls["avalanche"]))
        task = asyncio.create_task(
            get_current_block(web3)
        )
        task.add_done_callback(
            lambda t: asyncio.create_task(self._fetch_and_store_crabada_selling_transactions_aux(
                t.result()
            ))
        )
        asyncio.gather(task)

    async def _fetch_and_store_crabada_selling_transactions_aux(self, current_block):
        last_block_crabada_selling_transaction = max(self._get_variable("blocks_crabada_selling_transaction", lambda: {0}))
        last_block_seen = self._get_variable("last_block_seen_selling", lambda: 0)
        if current_block - last_block_seen >= NUMBER_BLOCKS_WAIT_BETWEEN_SNOWTRACE_CALLS:
            link_transactions = f"https://api.snowtrace.io/api?module=logs&action=getLogs&fromBlock={last_block_crabada_selling_transaction}&toBlock=99999999999&address=0x7e8deef5bb861cf158d8bdaaa1c31f7b49922f49&apikey={SNOWTRACE_API_KEY}&topic0=0x4d3b1cf93e7676f80b7df86edb68fdf7be63c9964cc44c6b43b51c434b8ab771"
            task = async_http_get_request_with_callback_on_result_v2(
                    link_transactions,
                    self._fetch_and_store_crabada_selling_transactions_web3,
                    TIMEOUT,
                    self._fetch_and_store_crabada_selling_entry,
                    callback_failure_args = (current_block, last_block_crabada_selling_transaction,),
                    semaphore=self._get_variable(f"sem_{SNOWTRACE_SEM_ID}", lambda: asyncio.Semaphore(value=1))
                )
            self._set_sync_variable("last_block_seen_selling", current_block)
            asyncio.gather(task)

    async def _fetch_and_store_crabada_selling_transactions_web3(self, e, current_block, last_block_crabada_transaction):
        task = asyncio.create_task(
            get_transactions_between_blocks_async(lambda: Web3(Web3.HTTPProvider(blockchain_urls["avalanche"])), last_block_crabada_transaction, end_block=current_block, filter_t=lambda t: is_valid_marketplace_selling_transaction(t), convert_to_logs=True, callback = self._fetch_and_store_crabada_selling_entry)
        )
        asyncio.gather(task)

    async def _fetch_and_store_crabada_selling_entry(self, logs):
        log = [log for log in logs if str(log["address"]).lower() == "0x7E8DEef5bb861cF158d8BdaAa1c31f7B49922F49".lower()]
        if log == []:
            return
        log = log[-1]
        token_id = int(log["data"][:66], 16)

        selling_price = int(round(int(log["data"][66:], 16) * 10**-18, 0))
        timestamp = log["timeStamp"]
        if "0x" in str(log["blockNumber"]):
            block_number = int(log["blockNumber"], 16)
        else:
            block_number =  int(log["blockNumber"])
        if "0x" in str(log["timeStamp"]):
            timestamp = int(log["timeStamp"], 16)
        else:
            timestamp =  int(log["timeStamp"])
        link_nft_crabada = f"https://api.crabada.com/public/crabada/info/{token_id}"
        already_seen = self._get_variable("already_seen", lambda: set())
        if (token_id, timestamp, selling_price, True) not in already_seen:
            print(f"spotted selling item {token_id}")
            task = async_http_get_request_with_callback_on_result_v2(
                            link_nft_crabada,
                            self._recall_crabada_api_after_sleep,
                            TIMEOUT,
                            self._fetch_and_store_crabada_entry_aux,
                            callback_failure_args=(token_id, block_number, selling_price, timestamp),
                            callback_failure_kwargs={"is_selling": True},
                            f_args=(block_number, selling_price, token_id, timestamp),
                            f_kwargs={"is_selling": True},
                            semaphore=self._get_variable(f"sem_{APICRABADA_SEM_ID}", lambda: asyncio.Semaphore(value=1))
                        )
            asyncio.gather(task)

    """
    COMMON
    """

    async def _fetch_and_store_crabada_entry_aux(self, infos_nft, block_number, selling_price, token_id, timestamp, is_selling=True):
        type_entry = "selling" if is_selling else "listing"
        async with self._get_variable(f"last_block_crabada_{type_entry}_transaction_semaphore_{token_id}_{timestamp}_{selling_price}", lambda: asyncio.Semaphore(value=1)):
            blocks_crabada_transaction = self._get_variable(f"blocks_crabada_{type_entry}_transaction", lambda: {0})
            self._set_sync_variable(f"blocks_crabada_{type_entry}_transaction", blocks_crabada_transaction.union({block_number}))
            already_seen = self._get_variable("already_seen", lambda: set())
            if (token_id, timestamp, selling_price, is_selling) not in already_seen:
                self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp, selling_price, is_selling)}))
                if is_crab(infos_nft):

                    task = asyncio.create_task(self._store_crabada_entry_crab_aux(infos_nft, token_id, selling_price, timestamp, is_selling=is_selling))
                else:
                    family_infos_link = f"https://api.crabada.com/public/crabada/family/{token_id}"
                    task = async_http_get_request_with_callback_on_result_v2(
                            family_infos_link,
                            lambda e: self._set_variable("already_seen", already_seen.difference({(token_id, timestamp, selling_price, is_selling)})),
                            TIMEOUT,
                            self._store_crabada_entry_egg_aux,
                            f_args=(token_id, selling_price, timestamp, infos_nft),
                            f_kwargs={"is_selling": is_selling},
                            semaphore=self._get_variable(f"sem_{APICRABADA_SEM_ID}", lambda: asyncio.Semaphore(value=1)),
                        )
                    
                asyncio.gather(task)

    async def _store_crabada_entry_crab_aux(self, infos_nft, token_id, selling_price, timestamp, is_selling=True):
        table_name = "crabada_sellings" if is_selling else "crabada_listings"
        
        query = f"""
            INSERT OR REPLACE INTO {table_name} (token_id, selling_price, timestamp, infos_nft, infos_family, is_crab) VALUES ({token_id}, {selling_price}, {timestamp}, '{json.dumps(infos_nft)}', '', 'TRUE')
            """
        type_entry = "selling" if is_selling else "listing"
        print(f"crab to be spotted {token_id} {type_entry}")
        async with self._get_variable(f"sem_database_{is_selling}", lambda: asyncio.Semaphore(value=1)):
            db = open_database()
            execute_query(
                db,
                query
            )
            close_database(db)
            
            print(f"crab spotted {token_id} {type_entry}")

    async def _store_crabada_entry_egg_aux(self, infos_family, token_id, selling_price, timestamp, infos_nft, is_selling=True):
        table_name = "crabada_sellings" if is_selling else "crabada_listings"
        
        query = f"""
            INSERT OR REPLACE INTO {table_name} (token_id, selling_price, timestamp, infos_nft, infos_family, is_crab) VALUES ({token_id}, {selling_price}, {timestamp}, '{json.dumps(infos_nft)}', '{json.dumps(infos_family)}', 'FALSE')
            """
        type_entry = "selling" if is_selling else "listing"
        print(f"egg to be spotted {token_id} {type_entry}")
        async with self._get_variable(f"sem_database_{is_selling}", lambda: asyncio.Semaphore(value=1)):
            db = open_database()
            execute_query(
                db,
                query
            )
            close_database(db)
            print(f"egg spotted {token_id} {type_entry}")

    async def _fetch_and_store_crabada_transactions_loop(self, seconds=1):
        while True:
            await asyncio.sleep(seconds)
            task_listing = asyncio.create_task(self._fetch_and_store_crabada_listing_transactions())
            task_selling = asyncio.create_task(self._fetch_and_store_crabada_selling_transactions())
            asyncio.gather(task_listing, task_selling)

    async def run(self):
        fetch_and_store_crabada_transactions_task = asyncio.create_task(self._fetch_and_store_crabada_transactions_loop())
        fetch_and_store_payments_loop_task = asyncio.create_task(self._fetch_and_store_payments_loop())
        await asyncio.gather(
            fetch_and_store_crabada_transactions_task,
            fetch_and_store_payments_loop_task
        )

        #asyncio.create_task(self._run_fetch_and_store_crabada_selling_transactions_loop())
        #asyncio.create_task(self._run_fetch_and_store_crabada_listing_transactions_loop())
            

if __name__ == "__main__":
    asyncio.run(Crabfetcher().run())

