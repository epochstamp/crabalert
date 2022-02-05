from datetime import datetime, timezone, timedelta
import aiohttp
from web3.main import Web3
from pprint import pprint
from discord.ext import tasks
from config import (
    COINS_SYMBOL,
    CRABALERT_SEM_ID,
    ID_COMMAND_CENTER,
    channel_to_post_with_filters,
    APICRABADA_SEM_ID,
    TIMEOUT,
    channels_to_display_shortdescrs,
    cool_classes,
    cool_subclasses,
    THRESOLD_PURE_PROBA,
    channels_emojis,
    NUMBER_BLOCKS_WAIT_BETWEEN_SNOWTRACE_CALLS,
    SNOWTRACE_SEM_ID,
    CRABREFRESH_SEM_ID,
    CRABMESSAGE_SEM_ID,
    EGGMESSAGE_SEM_ID,
    SNOWTRACE_API_KEY,
    PAYMENT_SEM_ID,
    ID_TUS_BOT,
    ID_SERVER,
    COINS,
    TUS_CONTRACT_ADDRESS,
    stablecoins,
    MINIMUM_PAYMENT,
    HEADERS,
    SPAN_TIMESTAMP,
    ADFLY_SEM_ID,
    subclass_map,
    MONTHLY_RATE)
from utils import (
    async_http_get_request_with_callback_on_result,
    blockchain_urls,
    get_token_price_from_dexs,
    is_crab,
    is_valid_marketplace_transaction,
    open_database,
    close_database,
    execute_query,
    iblock_near,
    get_transactions_between_blocks,
    get_current_block
)
from discord.utils import get
import humanize
from discord.ext import commands
import asyncio
import urllib
import json
from eggs_utils import calc_pure_probability
from classes import classes_to_spacebarsize_map


class Crabalert(commands.Bot):
    def __init__(self, command_prefix="!", intents=None, variables=None, crabalert_observers=[]):
        super().__init__(command_prefix=command_prefix, intents=intents)
        self._crabalert_observers = crabalert_observers
        if variables is None:
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
                last_block_crabada_transaction = max([block_number_ago]+[int(transaction["blockNumber"]) for transaction in json.loads(urllib.request.urlopen(req).read())["result"] if isinstance(transaction, dict) and is_valid_marketplace_transaction(transaction)])
            except:
                transactions = asyncio.run(get_transactions_between_blocks(web3, block_number_ago, filter_t=lambda t: is_valid_marketplace_transaction(t)))
                last_block_crabada_transaction = max([block_number_ago]+[int(transaction["blockNumber"]) for transaction in transactions if isinstance(transaction, dict) and is_valid_marketplace_transaction(transaction)])
            self._variables = {
                "block_number_ago": block_number_ago,
                "last_block_seen": block_number_ago,
                "last_block_crabada_transaction": last_block_crabada_transaction,
                "web3": web3
            }
            asyncio.run(
                self._refresh_prices_coin()
            )
        else:
            self._variables = variables
        self._launched = False
        self._tasks = []
        
    @property
    def variables(self):
        return self._variables

    def _get_variable(self, name: str, f_value_if_not_exists=lambda: None):
        if name not in self._variables:
            self._variables[name] = f_value_if_not_exists()
        return self._variables[name]

    async def _set_sync_variable(self, name: str, value):
        self._variables[name] = value

    def _set_sync_variable(self, name: str, value):
        self._variables[name] = value

    async def _close_all_tasks(self):
        for task in self._tasks:
            try:
                await task.close()
            except:
                pass
        
    async def on_ready(self):
        if not self._launched:
            channel = self._get_variable(f"channel_{ID_COMMAND_CENTER}", f_value_if_not_exists=lambda: self.get_channel(ID_COMMAND_CENTER))
            asyncio.create_task(channel.send("Hi ! I'm back."))
            self._refresh_tus_price()
            self._tasks.append(self._refresh_crabada_transactions_loop.start())
            self._tasks.append(self._crabada_alert_loop.start())
            self._tasks.append(self._refresh_tus_loop.start())
            self._tasks.append(self._refresh_prices_coin_loop.start())
            self._tasks.append(self._manage_alerted_roles.start())
            self._launched = True

    def _get_members(self):
        return self.get_guild(ID_SERVER).members

    @tasks.loop(minutes=10)
    async def _manage_alerted_roles(self):
        guild = self.get_guild(ID_SERVER)
        role_alerted = get(guild.roles, name="Alerted")

        for member in self._get_members():
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
                try:
                    connection = open_database()
                except Exception as e:
                    #TODO : logging
                    return
                dt = datetime.now(timezone.utc)
                utc_time = dt.replace(tzinfo=timezone.utc)
                current_timestamp = utc_time.timestamp()
                
                data = list(execute_query(
                    connection, f"SELECT * FROM trials WHERE discord_id = {member.id}"
                ))
                rowcount = len(data)
                if rowcount > 0:
                    start_trial, duration_trial = data[0][1:]
                    if current_timestamp - datetime.strptime(start_trial, "%Y-%m-%d %H:%M:%S").astimezone(timezone.utc).timestamp() <= duration_trial:
                        if "Alerted" not in roles_str:
                            asyncio.create_task(member.add_roles(role_alerted))
                        continue
                    else:
                        if "Alerted" in roles_str:
                            asyncio.create_task(member.remove_roles(role_alerted))


                data = list(execute_query(
                    connection, f"SELECT * FROM last_received_payment WHERE discord_id = {member.id}"
                ))
                rowcount = len(data)
                close_database(connection)
                if "Alerted" in roles_str and rowcount == 0:
                    asyncio.create_task(member.remove_roles(role_alerted))
                
                if rowcount > 0:
                    wallet_address, payment_date, duration, txhash, reminded = data[0][1:]
                    if duration < 0:
                        if "Alerted" not in roles_str:
                            asyncio.create_task(member.add_roles(role_alerted))
                    else:
                        reminded = reminded.lower() == "true"
                        if payment_date == 0:
                            payment_timestamp = int(round(current_timestamp, 0)) - 3600
                        else:
                            payment_timestamp = int(round(datetime.fromtimestamp(int(payment_date)).astimezone(timezone.utc).timestamp(), 0))
                        asyncio.create_task(self._manage_alert_roles_from_payments(member, wallet_address, payment_timestamp))
                        """
                        task.add_done_callback(
                            lambda t: asyncio.create_task(self._manage_alerted_roles_aux(t, current_timestamp, payment_timestamp, guild, member, duration, roles_str, reminded, connection))
                        )
                        """

                

    async def _manage_alerted_roles_aux(self, transactions, wallet_address, payment_timestamp, contract_address, member):
        async with self._get_variable(f"sem_{PAYMENT_SEM_ID}_{member.id}_{contract_address}", lambda: asyncio.Semaphore(value=1)):
            connection = open_database()
            data = list(execute_query(
                        connection, f"SELECT duration, reminded FROM last_received_payment WHERE discord_id = {member.id}"
                    ))
            #print(member, data)
            rowcount = len(data)
            if rowcount == 0:
                return
            dt = datetime.now(timezone.utc)
            utc_time = dt.replace(tzinfo=timezone.utc)
            current_timestamp = utc_time.timestamp()
            duration, reminded = tuple(data[0])
            guild = self.get_guild(ID_SERVER)
            roles_str = [str(role) for role in member.roles]
            
            payments = self._fetch_payments_coin_from_aux(wallet_address, payment_timestamp, contract_address, transactions)
            role_alerted = get(guild.roles, name="Alerted")
            
            if current_timestamp - int(payment_timestamp) > duration:
                if payments == [] and "Alerted" in roles_str:
                    asyncio.create_task(member.remove_roles(role_alerted))
                    asyncio.create_task(member.send("Your subscription to Crabalert has expired and your access to the alerts removed."))
                    asyncio.create_task(member.send("If you want to renew it, please send payments again (see #instructions for a soft reminder)"))
            else:
                #Send reminders if subscription is close (< 1h)
                remaining_time = (int(payment_timestamp) + int(duration)) - current_timestamp
                if remaining_time <= 3600*24 and not reminded:
                    remaining_deltatime = humanize.naturaltime(current_timestamp + timedelta(seconds = remaining_time))
                    asyncio.create_task(member.send(
                        f"Your subscription to Crabalert is going to expire in less than one day (remaining:{remaining_deltatime}). If you want to keep it, please send payment again (see #instructions for a soft reminder)"
                    ))
                    update_query = f"UPDATE last_received_payment SET reminded='TRUE' WHERE discord_id={member.id}"
                    execute_query(connection, update_query)
                if "Alerted" not in roles_str:
                    asyncio.create_task(member.add_roles(role_alerted))
            if payments != []:
                for payment, trans_timestamp in payments:
                    new_duration = payment/MONTHLY_RATE * 3600 * 24 * 30 + max((int(payment_timestamp) + int(duration)) - current_timestamp, 0)
                    trans_timestamp_date = datetime.fromtimestamp(trans_timestamp, timezone.utc)
                    update_query = f"UPDATE last_received_payment SET duration={int(round(new_duration, 0))}, received_timestamp='{current_timestamp}', reminded='FALSE' WHERE discord_id={member.id}"
                    execute_query(connection, update_query)
                    connection.commit()
                    if "Alerted" not in roles_str:
                        asyncio.create_task(member.add_roles(role_alerted))
                    delta_duration = timedelta(seconds = new_duration + 3600*24*30)
                    current_timestamp_datetime = datetime.fromtimestamp(current_timestamp, timezone.utc)
                    human_friendly_duration = humanize.naturaldelta(current_timestamp_datetime - (current_timestamp_datetime+delta_duration), when=current_timestamp_datetime)
                    asyncio.create_task(member.send(f"Your payment of {payment} {COINS_SYMBOL.get(contract_address.lower(), '???')} received at {trans_timestamp_date.strftime('%d, %b %Y')} has been checked and your subscription has just been extended for a duration of {human_friendly_duration}."))
            try:
                close_database(connection)
            except:
                pass

    def _fetch_payments_coin_from_aux(self, wallet_address, from_timestamp, contract_address, transactions):
        decimals = COINS.get(contract_address.lower(), 18)
        wallet_transactions = transactions
        wallet_transactions = [w for w in wallet_transactions if int(w["timeStamp"]) > int(from_timestamp) and w["from"].lower() == wallet_address.lower()]
        rate = 1 if contract_address.lower() in stablecoins else 1.3
        price_coins = self._get_variable("price_coins", lambda: dict())
        price_in_usd = price_coins.get(contract_address.lower(), -1)
        lst = [((int(w["value"])*price_in_usd*10**-decimals)/rate, int(w["timeStamp"])) for w in wallet_transactions if (int(w["value"])*10**-decimals*price_in_usd)/rate >= MINIMUM_PAYMENT]
        return lst

    async def _manage_alert_roles_from_payments(self, member, wallet_address, payment_timestamp):
        asyncio.create_task(async_http_get_request_with_callback_on_result(
            f"https://api.snowtrace.io/api?module=block&action=getblocknobytime&timestamp={payment_timestamp}&closest=before&apikey={SNOWTRACE_API_KEY}",
            lambda e: self._manage_alert_roles_from_payments_web3(self._get_variable("web3", lambda: Web3(Web3.HTTPProvider(blockchain_urls["avalanche"]))), member, wallet_address, payment_timestamp),
            TIMEOUT,
            lambda r: self._manage_alert_roles_from_payments_aux(member, wallet_address, payment_timestamp, r.result()),
            semaphore=self._get_variable(f"sem_{SNOWTRACE_SEM_ID}", lambda: asyncio.Semaphore(value=2))
        ))

    async def _manage_alert_roles_from_payments_web3(self, web3, member, wallet_address, payment_timestamp):
        block = asyncio.create_task(iblock_near(web3, payment_timestamp))
        block.add_done_callback(lambda t: asyncio.create_task(self._manage_alert_roles_from_payments_aux(member, wallet_address, payment_timestamp, t.result())))

    async def _manage_alerted_roles_aux_web3(self, web3, member, wallet_address, payment_timestamp, contract_address, block_number):
        task = asyncio.create_task(get_transactions_between_blocks(
                web3,
                block_number,
                filter_t = lambda t: (
                    t["from"].lower() == wallet_address.lower() and
                    t["to"].lower() == contract_address.lower() and
                    t["input"][34:74].lower() == "0xbda6ffd736848267afc2bec469c8ee46f20bc342".lower() and
                    int(t["timeStamp"]) > payment_timestamp
                )
            )
        )
        task.add_done_callback(
            lambda t: asyncio.create_task(self._manage_alerted_roles_aux({
                {**transaction, **{"value": int(transaction["input"][74:], 16)}} for transaction in t.result()
            }, wallet_address, payment_timestamp, contract_address, member))
        )
        

    async def _manage_alert_roles_from_payments_aux(self, member, wallet_address, payment_timestamp, block_number):
        for contract_address in COINS.keys():
            wallet_transactions_link = f"https://api.snowtrace.io/api?module=account&action=tokentx&contractaddress={contract_address}&address=0xbda6ffd736848267afc2bec469c8ee46f20bc342&startblock={block_number}&sort=desc&endblock=999999999999&apikey={SNOWTRACE_API_KEY}"
            lst = await async_http_get_request_with_callback_on_result(
                wallet_transactions_link,
                lambda e: self._manage_alerted_roles_aux_web3(self._get_variable("web3", lambda: Web3(Web3.HTTPProvider(blockchain_urls["avalanche"]))), member, wallet_address, payment_timestamp, contract_address, block_number),
                TIMEOUT,
                lambda r: self._manage_alerted_roles_aux(r, wallet_address, payment_timestamp, contract_address, member),
                semaphore=self._get_variable(f"sem_{SNOWTRACE_SEM_ID}", lambda: asyncio.Semaphore(value=2))
            )

    """
    async def _fetch_payments_coin_from_aux(self, wallet_address, from_timestamp, contract_address, transactions):
        decimals = COINS.get(contract_address.lower(), 18)
        wallet_transactions = transactions
        wallet_transactions = [w for w in wallet_transactions if int(w["timeStamp"]) > int(from_timestamp) and w["from"].lower() == wallet_address.lower()]
        rate = 1 if contract_address.lower() in stablecoins else 1.3
        price_coins = self._get_variable("price_coins", lambda: dict())
        price_in_usd = price_coins.get(contract_address.lower(), -1)
        lst = [(int(w["value"])*price_in_usd*10**-decimals)/rate for w in wallet_transactions if (int(w["value"])*10**-decimals*price_in_usd)/rate >= MINIMUM_PAYMENT]
        return lst

    async def _fetch_payments_coin_from(self, wallet_address, from_timestamp, contract_address, previous_block_number):
        wallet_transactions_link = f"https://api.snowtrace.io/api?module=account&action=tokentx&contractaddress={contract_address}&address=0xbda6ffd736848267afc2bec469c8ee46f20bc342&startblock={previous_block_number}&sort=desc&endblock=999999999999&apikey={SNOWTRACE_API_KEY}"
        lst = await async_http_get_request_with_callback_on_result(
            wallet_transactions_link,
            lambda e: self._fetch_payments_coin_from_web3(self._get_variable("web3", lambda: Web3(Web3.HTTPProvider(blockchain_urls["avalanche"]))), wallet_address, from_timestamp, contract_address, previous_block_number),
            TIMEOUT,
            lambda r: self._fetch_payments_coin_from_aux(wallet_address, from_timestamp, contract_address, r),
            semaphore=self._get_variable(f"sem_{SNOWTRACE_SEM_ID}", lambda: asyncio.Semaphore(value=2))
        )
        return lst

    async def _fetch_payments_coin_from_web3(self, web3, wallet_address, from_timestamp, contract_address, previous_block_number):
        task = asyncio.create_task(get_transactions_between_blocks(
                web3,
                previous_block_number,
                filter_t = lambda t: (
                    t["from"].lower() == wallet_address.lower() and
                    t["to"].lower() == contract_address.lower() and
                    t["input"][34:74].lower() == "0xbda6ffd736848267afc2bec469c8ee46f20bc342".lower() and
                    int(t["timeStamp"]) > int(from_timestamp)
                )
            )
        )

        transactions = {
            {**transaction, **{"value": int(transaction["input"][74:], 16)}} for transaction in transactions
        }
        return self._fetch_payments_coin_from_aux(wallet_address, from_timestamp, contract_address, transactions)
        

    async def _fetch_payments_from_aux(self, wallet_address, from_timestamp, r, callback, *c_args, **c_kwargs):
        price_coins = self._get_variable("price_coins", lambda: dict())
        tasks = [asyncio.create_task(self._fetch_payments_coin_from(wallet_address, from_timestamp, coin, int(r))) for coin in COINS.keys() if price_coins.get(coin.lower(), -1) != -1]
        tasks = tuple(tasks)
        task = asyncio.gather(*tasks)
        task.add_done_callback(
            lambda t: callback(t.result(), *c_args, **c_kwargs)
        )

    async def _fetch_payments_from_web3(self, wallet_address, from_timestamp, callback):
        block = asyncio.create_task(iblock_near(self._get_variable("web3", lambda: Web3(Web3.HTTPProvider(blockchain_urls["avalanche"]))), from_timestamp))
        block.add_done_callback(lambda t: self._fetch_payments_from_aux(wallet_address, from_timestamp, t.result(), callback))

    async def _fetch_payments_from(self, wallet_address, from_timestamp, callback, *c_args, **c_kwargs):
        global headers
        lst = await async_http_get_request_with_callback_on_result(
            f"https://api.snowtrace.io/api?module=block&action=getblocknobytime&timestamp={from_timestamp}&closest=before&apikey={SNOWTRACE_API_KEY}",
            lambda e: self._fetch_payments_from_web3(wallet_address, from_timestamp, callback, *c_args, **c_kwargs),
            TIMEOUT,
            lambda r: self._fetch_payments_from_aux(wallet_address, from_timestamp, r, callback, *c_args, **c_kwargs),
            semaphore=self._get_variable(f"sem_{SNOWTRACE_SEM_ID}", lambda: asyncio.Semaphore(value=2))
        )
    """



    @tasks.loop(seconds=1)
    async def _refresh_tus_loop(self):
        self._refresh_tus_price()
        
    def _refresh_tus_price(self):
        server = self.get_guild(ID_SERVER)
        tus_bot = server.get_member(ID_TUS_BOT)
        price = float(tus_bot.nick.split(" ")[0][1:])
        self._set_sync_variable("price_tus", price)
        self._set_sync_variable(
            "price_coins",
            {
                **{TUS_CONTRACT_ADDRESS.lower(): self._get_variable("price_tus", lambda: -1)}, **self._get_variable("price_coins", dict())
            }
        )

    @tasks.loop(minutes=1)
    async def _refresh_prices_coin_loop(self):
        asyncio.create_task(self._refresh_prices_coin())

    async def _refresh_prices_coin(self):
        coins_keys = [k for k in COINS.keys() if k.lower() != TUS_CONTRACT_ADDRESS.lower()]
        tasks = tuple([get_token_price_from_dexs(self._get_variable("web3", lambda: Web3(Web3.HTTPProvider(blockchain_urls["avalanche"]))), "avalanche", c) for c in coins_keys])

        task = asyncio.gather(*tasks)
        task.add_done_callback(
            lambda t: self._set_sync_variable(
                "price_coins",
                {
                    **{TUS_CONTRACT_ADDRESS.lower(): self._get_variable("price_tus", lambda: -1)}, **{coins_keys[i]: t.result()[i] for i in range(len(coins_keys))}
                }
            )
        )

    @tasks.loop(seconds=1)
    async def _refresh_crabada_transactions_loop(self):
        #await refresh_crabada_transactions()
        web3 = self._get_variable("web3", f_value_if_not_exists=lambda: Web3(Web3.HTTPProvider(blockchain_urls["avalanche"])))
        task = asyncio.create_task(
            get_current_block(web3)
        )
        task.add_done_callback(
            lambda t: asyncio.create_task(self._refresh_crabada_transactions_loop_aux(
                t.result()
            ))
        )

    async def _refresh_crabada_transactions_loop_aux(self, current_block):
        last_block_crabada_transaction = self._get_variable("last_block_crabada_transaction", lambda: 0)
        last_block_seen = self._get_variable("last_block_seen", lambda: 0)
        web3 = self._get_variable("web3", f_value_if_not_exists=lambda: Web3(Web3.HTTPProvider(blockchain_urls["avalanche"])))
        if current_block - last_block_seen >= NUMBER_BLOCKS_WAIT_BETWEEN_SNOWTRACE_CALLS:
            last_block_crabada_transaction = self._get_variable("last_block_crabada_transaction", lambda: 0)
            link_transactions = f"https://api.snowtrace.io/api?module=account&action=txlist&address=0x1b7966315eF0259de890F38f1bDB95Acc03caCdD&startblock={last_block_crabada_transaction}&sort=desc&apikey={SNOWTRACE_API_KEY}"
            asyncio.create_task(async_http_get_request_with_callback_on_result(
                    link_transactions,
                    lambda e: self._refresh_crabada_transactions_web3(web3, current_block, last_block_crabada_transaction),
                    TIMEOUT,
                    lambda r: self._refresh_crabada_transactions(r, current_block),
                    semaphore=self._get_variable(f"sem_{SNOWTRACE_SEM_ID}", lambda: asyncio.Semaphore(value=2))
                )
            )

    async def _refresh_crabada_transactions_web3(self, web3, current_block, last_block_crabada_transaction):
        task = asyncio.create_task(
            get_transactions_between_blocks(web3, last_block_crabada_transaction, end_block=current_block, filter_t=lambda t: is_valid_marketplace_transaction(t))
        )
        task.add_done_callback(
            lambda t: asyncio.create_task(self._refresh_crabada_transactions(t.result(), current_block))
        )

    async def _refresh_crabada_transactions(self, crabada_transactions, current_block):
        asyncio.create_task(self._set_sync_variable("last_block_seen", current_block))
        crabada_transactions_lst = [transaction for transaction in crabada_transactions if isinstance(transaction, dict) and is_valid_marketplace_transaction(transaction)]
        crabada_transactions = self._get_variable(f"crabada_transactions", f_value_if_not_exists=lambda:[])
        dict_transaction_id = {transaction["hash"]:transaction for transaction in crabada_transactions_lst}
        crabada_transactions += list(dict_transaction_id.values())
        asyncio.create_task(self._set_sync_variable("crabada_transactions", sorted(crabada_transactions, key=lambda x: int(x["blockNumber"]))))
        if crabada_transactions != []:
            asyncio.create_task(self._set_sync_variable("last_block_crabada_transaction", int(crabada_transactions[-1]["blockNumber"]) + 1))

    @tasks.loop(seconds=1)
    async def _crabada_alert_loop(self):
        crabada_transactions = list(self._get_variable(f"crabada_transactions", f_value_if_not_exists=lambda:[]))
        set_transaction_id = {transaction["hash"] for transaction in crabada_transactions}
        asyncio.create_task(self._set_sync_variable("crabada_transactions", [transaction for transaction in crabada_transactions if transaction["hash"] not in set_transaction_id]))
        for transaction in crabada_transactions:
            set_transaction_id.add(transaction["hash"])
            asyncio.create_task(self._handle_crabada_transaction(transaction))

    async def _handle_crabada_transaction(self, crabada_transaction):
        input_data = crabada_transaction["input"]
        token_id = int(input_data[138:202], 16)
        price = int(round(int(input_data[330:], 16) * 10**-18, 0))
        link_nft_crabada = f"https://api.crabada.com/public/crabada/info/{token_id}"
        timestamp_transaction = int(crabada_transaction["timeStamp"])
        asyncio.create_task(
            async_http_get_request_with_callback_on_result(
                link_nft_crabada,
                lambda e: self._set_sync_variable("crabada_transactions", self._get_variable(f"crabada_transactions", f_value_if_not_exists=lambda:[]) + [crabada_transaction]),
                TIMEOUT,
                lambda r: self._notify_marketplace_item(r, token_id, price, timestamp_transaction, crabada_transaction),
                semaphore=self._get_variable(f"sem_{APICRABADA_SEM_ID}", lambda: asyncio.Semaphore(value=2))
            )
        )

    async def _notify_marketplace_item(self, r, token_id, price, timestamp_transaction, crabada_transaction):
        if is_crab(r):
            for channel_id, filter_function in channel_to_post_with_filters.items():
                channel = self._get_variable(f"channel_{channel_id}", f_value_if_not_exists=lambda:self.get_channel(channel_id))
                if filter_function((r, None)):
                    asyncio.create_task(
                        self.notify_crab_item(
                            r,
                            token_id,
                            price,
                            timestamp_transaction,
                            channel,
                            shortdescr=channel_id in channels_to_display_shortdescrs
                        )
                    )
            for observer in self._crabalert_observers:
                already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
                if (token_id, timestamp_transaction, observer.id) not in already_seen:
                   task = asyncio.create_task(observer.notify_crab_item(r, token_id, price, timestamp_transaction))
                   task.add_done_callback(lambda t: asyncio.create_task(self._set_sync_variable("already_seen", self._get_variable("already_seen").union({(token_id, timestamp_transaction, observer.id)}))))
        else:
            family_infos_link = f"https://api.crabada.com/public/crabada/family/{token_id}"
            asyncio.create_task(
                async_http_get_request_with_callback_on_result(
                    family_infos_link,
                    lambda e: self._set_sync_variable("crabada_transactions", self._get_variable(f"crabada_transactions", f_value_if_not_exists=lambda:[]) + [crabada_transaction]),
                    TIMEOUT,
                    lambda r2: self.notify_egg_item(r2, r, token_id, price, timestamp_transaction),
                    semaphore=self._get_variable(f"sem_{APICRABADA_SEM_ID}", lambda: asyncio.Semaphore(value=2)),
                )
            )

    async def notify_crab_item(self, infos_nft, token_id, price, timestamp_transaction, channel, shortdescr=False):
        async with self._get_variable(f"sem_{CRABMESSAGE_SEM_ID}_{token_id}_{timestamp_transaction}_{channel.id}", lambda: asyncio.Semaphore(value=1)):
            already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
            # print"crab from timestamp", timestamp_transaction,"will maybe be posted", token_id, "at channel", channel.id)
            if (token_id, timestamp_transaction, channel.id) not in already_seen:
                # print"crab from timestamp", timestamp_transaction,"will be posted", token_id, "at channel", channel.id)
                price_tus = self._get_variable(f"price_tus", f_value_if_not_exists=lambda:-1)
                price_usd = round(price * price_tus, 2)
                channel_id = channel.id
                tus_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("tus", ":tus:")
                crabadegg_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crabadegg", "crabadegg")
                tus_text = f"{tus_emoji} **{price}**"
                tus_text_len_in_space_bars = sum([1 if c == "1" else 2 for c in str(price)]) + 6 + 1
                usd_text = f":moneybag: **{price_usd}**"
                purity_text = (':gem: **PURE**' if infos_nft['pure_number'] == 6 else ':diamond_shape_with_a_dot_inside: ' + str(infos_nft['pure_number']))
                purity_text_len_in_space_bars = 3 + 1 + (12 if infos_nft['pure_number'] == 6 else (1 if infos_nft['pure_number'] == 1 else 2))
                breed_text = f"{crabadegg_emoji} {infos_nft['breed_count'] if infos_nft['breed_count'] > 0 else '**NO-BREED**'}"
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
                if shortdescr:
                    async with self._get_variable(f"sem_{ADFLY_SEM_ID}_{token_id}_{channel.id}", f_value_if_not_exists=lambda:asyncio.Semaphore(value=1)) as sem:
                        message = (
                            f":crab: {class_display}({subclass_display})\n" +
                            f"{first_column}\n" +
                            "<marketplace_link>"
                        )
                        asyncio.create_task(
                            self._send_crab_item_message(token_id, timestamp_transaction, channel, already_seen, message, marketplace_link)
                        )
                        
                        
                else:
                    message = (
                        f":crab: {'**PURE**' if infos_nft['pure_number'] == 6 else ''}{' **ORIGIN**' if infos_nft['is_origin'] == 1 else ''}{' **NO-BREED**' if infos_nft['breed_count'] == 0 else ''} {class_display}({subclass_display})\n" +
                        f"{first_column}\n" +
                        f"{second_column}\n" +
                        f"{third_column}\n" +
                        f"https://photos.crabada.com/{token_id}.png\n" +
                        "<marketplace_link>"
                    )
                    asyncio.create_task(
                        self._send_crab_item_message(token_id, timestamp_transaction, channel, already_seen, message, marketplace_link)
                    )

    async def notify_egg_item_channel(self, infos_family_nft, token_id, price, timestamp_transaction, channel):
        async with self._get_variable(f"sem_{EGGMESSAGE_SEM_ID}_{token_id}_{timestamp_transaction}_{channel.id}", lambda: asyncio.Semaphore(value=1)):
            channel_id = channel.id
            price_tus = self._get_variable(f"price_tus", f_value_if_not_exists=lambda:-1)
            price_usd = round(price * price_tus, 2)
            infos_family_nft = infos_family_nft["crabada_parents"]
            crabada_parent_1 = infos_family_nft[0]
            crabada_parent_2 = infos_family_nft[1]
            class_parent_1 = crabada_parent_1["class_name"]
            class_parent_2 = crabada_parent_2["class_name"]
            dna_parent_1 = crabada_parent_1["dna"]
            dna_parent_2 = crabada_parent_2["dna"]
            egg_purity_probability = round(calc_pure_probability(dna_parent_1, dna_parent_2, class_parent_1), 4)
            if egg_purity_probability.is_integer():
                egg_purity_probability = int(egg_purity_probability)
            tus_text = f"<tus> **{price}**"
            tus_text_len_in_space_bars = sum([1 if c == "1" else 2 for c in str(price)]) + 6 + 1
            usd_text = f":moneybag: **{price_usd}**"
            marketplace_link = f"https://marketplace.crabada.com/crabada/{token_id}"
            
            if class_parent_1 == class_parent_2:
                egg_class = class_parent_1
                egg_class_display = egg_class if egg_class.lower() not in cool_classes else f"**{egg_class}**"
                
                emoji_pure = ":gem:" if egg_purity_probability >= THRESOLD_PURE_PROBA else ":diamond_shape_with_a_dot_inside:"
                probability_display = f"**{round(egg_purity_probability*100, 2)}%**" if egg_purity_probability >= THRESOLD_PURE_PROBA else f"{round(egg_purity_probability*100, 2)}%"
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
                egg_purity_probability_2 = round(calc_pure_probability(dna_parent_1, dna_parent_2, class_parent_1), 4)
                if egg_purity_probability_2.is_integer():
                    egg_purity_probability_2 = int(egg_purity_probability_2)
                egg_class_1 = class_parent_1
                egg_class_2 = class_parent_2
                egg_class_display_1 = egg_class_1 if egg_class_1.lower() not in cool_classes else f"**{egg_class_1}**"
                egg_class_display_2 = egg_class_2 if egg_class_2.lower() not in cool_classes else f"**{egg_class_2}**"
                egg_class_display = f"({egg_class_display_1}┃{egg_class_display_2})"
                egg_class_text_1 = f"<crab1> {egg_class_display_1}"
                egg_class_text_2 = f"<crab2> {egg_class_display_2}"
                egg_class_1_text_len_in_space_bars = 4 + 1 + classes_to_spacebarsize_map.get(class_parent_1.upper(), 1)
                emoji_pure_1 = ":gem:" if egg_purity_probability_1 >= THRESOLD_PURE_PROBA else ":diamond_shape_with_a_dot_inside:"
                emoji_pure_2 = ":gem:" if egg_purity_probability_2 >= THRESOLD_PURE_PROBA else ":diamond_shape_with_a_dot_inside:"
                probability_display_1 = f"**{round(egg_purity_probability_1*100, 2)}%**" if egg_purity_probability_1 >= THRESOLD_PURE_PROBA else f"{round(egg_purity_probability_1*100, 2)}%"
                probability_display_2 = f"**{round(egg_purity_probability_2*100, 2)}%**" if egg_purity_probability_2 >= THRESOLD_PURE_PROBA else f"{round(egg_purity_probability_2*100, 2)}%"
                purity_probability_text_1 = f"{emoji_pure_1} {probability_display_1}"
                purity_probability_text_1_len_in_space_bars = 4 + 1 + sum([1 if c == "1" or c == "." else 2 for c in str(int(round(egg_purity_probability_1*100, 2)))])
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
                probability_pure = round(0.5*egg_purity_probability_1 + 0.5*egg_purity_probability_2, 2)
                if probability_pure.is_integer():
                    probability_pure = int(probability_pure)
                infos_egg = {
                    "class_name_1": egg_class_1,
                    "class_name_2": egg_class_2,
                    "probability_pure": probability_pure
                }
            header_message = f"<crabadegg> {'**PURE** ' if infos_egg['probability_pure'] == 1 else ''}{egg_class_display} \n"
            footer_message = (
                f"https://i.ibb.co/hXcP49w/egg.png \n" +
                "<marketplace_link>"
            )
            crab_1_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crab1", ":crab1:")#"<:crab1:934087822254694441>" if channel_id == 932591668597776414 else "<:crab_1:934075767602700288>"
            crab_2_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crab2", ":crab2:")#"<:crab2:934087853732921384>" if channel_id == 932591668597776414 else "<:crab_2:934076410132332624>"
            tus_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("tus", ":tus:")
            crabadegg_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crabadegg", ":crabadegg:")
            if channel_id in channels_to_display_shortdescrs:
                async with self._get_variable(f"sem_{ADFLY_SEM_ID}_{token_id}_{channel.id}", f_value_if_not_exists=lambda:asyncio.Semaphore(value=1)) as sem:
                    message_egg = (
                        f"{first_column}\n"
                    )
                    footer_message_egg = (
                        "<marketplace_link>"
                    )
                    header_message_egg = (
                        f"<crabadegg> {egg_class_display} \n"
                    )
                    asyncio.create_task(
                        self._send_egg_item_message(message_egg, header_message_egg, footer_message_egg, crab_2_emoji, tus_emoji, crab_1_emoji, crabadegg_emoji, token_id, timestamp_transaction, channel, marketplace_link)
                    )
                    
            else:
                header_message_egg = header_message
                footer_message_egg = footer_message
                message_egg = message
                asyncio.create_task(
                    self._send_egg_item_message(message_egg, header_message_egg, footer_message_egg, crab_2_emoji, tus_emoji, crab_1_emoji, crabadegg_emoji, token_id, timestamp_transaction, channel, marketplace_link)
                )

    async def _send_crab_item_message(self, token_id, timestamp_transaction, channel, already_seen, message, marketplace_link):
        if (token_id, timestamp_transaction, channel.id) not in already_seen:
            task = asyncio.create_task(channel.send(message.replace("<marketplace_link>", marketplace_link)))
            task.add_done_callback(lambda t: asyncio.create_task(self._set_sync_variable("already_seen", self._get_variable("already_seen").union({(token_id, timestamp_transaction, channel.id)}))))

    async def _send_egg_item_message(self, message_egg_in, header_message_egg, footer_message_egg, crab_2_emoji, tus_emoji, crab_1_emoji, crabadegg_emoji, token_id, timestamp_transaction, channel, marketplace_link):
        message_egg = header_message_egg + message_egg_in + footer_message_egg
        message_egg = message_egg.replace("<crab1>", crab_1_emoji).replace("<crab2>", crab_2_emoji).replace("<tus>", tus_emoji).replace("<crabadegg>" ,crabadegg_emoji).replace("<marketplace_link>", marketplace_link)
        already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
        if (token_id, timestamp_transaction, channel.id) not in already_seen:
            task = asyncio.create_task(channel.send(message_egg))
            task.add_done_callback(lambda t: asyncio.create_task(self._set_sync_variable("already_seen", self._get_variable("already_seen").union({(token_id, timestamp_transaction, channel.id)}))))

    async def notify_egg_item(self, infos_family_nft, infos_nft, token_id, price, timestamp_transaction):
        for channel_id, filter_function in channel_to_post_with_filters.items():
            channel = self._get_variable(f"channel_{channel_id}", f_value_if_not_exists=lambda:self.get_channel(channel_id))
            already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
            # print"egg from timestamp", timestamp_transaction,"will maybe be posted", token_id, "at channel", channel.id)
            if (token_id, timestamp_transaction, channel.id) not in already_seen and filter_function((infos_nft, infos_family_nft)):
                asyncio.create_task(self.notify_egg_item_channel(infos_family_nft, token_id, price, timestamp_transaction, channel))
        for observer in self._crabalert_observers:
            already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
            if (token_id, timestamp_transaction, observer.id) not in already_seen:
                task = asyncio.create_task(observer.notify_egg_item(infos_family_nft, infos_nft, token_id, price, timestamp_transaction))
                task.add_done_callback(lambda t: asyncio.create_task(self._set_sync_variable("already_seen", self._get_variable("already_seen").union({(token_id, timestamp_transaction, observer.id)}))))

    async def _shorten_link(self, url):
        return url
