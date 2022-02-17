from datetime import datetime, timezone, timedelta
import os
import wget
from web3.main import Web3
from pprint import pprint
from discord.ext import tasks
from discord import Embed, File
from config import (
    COINS_SYMBOL,
    CRABALERT_SEM_ID,
    ID_COMMAND_CENTER,
    LISTING_ITEM_EXPIRATION,
    SELLING_ITEM_EXPIRATION,
    channel_to_post_listings_with_filters,
    channel_to_post_sellings_with_filters,
    APICRABADA_SEM_ID,
    TIMEOUT,
    listing_channels_to_display_shortdescrs,
    selling_channels_to_display_shortdescrs,
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
    is_valid_marketplace_listing_transaction,
    open_database,
    close_database,
    execute_query,
    iblock_near,
    get_transactions_between_blocks,
    get_current_block,
    seconds_to_pretty_print
)
from discord.utils import get
import humanize
from discord.ext import commands
import asyncio
import urllib
import json
from eggs_utils import calc_pure_probability
from classes import classes_to_spacebarsize_map


class CrabalertDiscord(commands.Bot):
    def __init__(self, command_prefix="!", intents=None, variables=None):
        super().__init__(command_prefix=command_prefix, intents=intents)
        self._launched = False
        self._tasks = []
        self._variables = variables if variables is not None else dict()
        asyncio.run(
            self._refresh_prices_coin()
        )
        
        
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

    async def _close_all_tasks(self):
        for task in self._tasks:
            try:
                await task.close()
            except:
                pass
        
    async def on_ready(self):
        if not self._launched:
            channel = self._get_variable(f"channel_{ID_COMMAND_CENTER}", f_value_if_not_exists=lambda: self.get_channel(ID_COMMAND_CENTER))
            asyncio.gather(asyncio.create_task(channel.send("Hi ! I'm back.")))
            self._refresh_tus_price()
            self._tasks.append(self._crabada_listing_alert_loop.start())
            self._tasks.append(self._crabada_selling_alert_loop.start())
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
        tasks = []
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
                            tasks.append(asyncio.create_task(member.add_roles(role_alerted)))
                        continue
                    else:
                        if "Alerted" in roles_str:
                            tasks.append(asyncio.create_task(member.remove_roles(role_alerted)))


                data = list(execute_query(
                    connection, f"SELECT * FROM last_received_payment WHERE discord_id = {member.id}"
                ))
                rowcount = len(data)

                if "Alerted" in roles_str and rowcount == 0:
                    tasks.append(asyncio.create_task(member.remove_roles(role_alerted)))
                
                if rowcount > 0:
                    wallet_address, payment_date, duration, _, reminded = data[0][1:]
                    if duration < 0:
                        if "Alerted" not in roles_str:
                            tasks.append(asyncio.create_task(member.add_roles(role_alerted)))
                    else:
                        reminded = reminded.lower() == "true"
                        if payment_date == 0:
                            payment_timestamp = int(round(current_timestamp, 0)) - 3600
                        else:
                            payment_timestamp = int(round(datetime.fromtimestamp(int(payment_date)).astimezone(timezone.utc).timestamp(), 0))
                        data = list(execute_query(
                            connection, f"SELECT received_timestamp, contract_address, txn_hash, value FROM payments WHERE from_wallet = '{wallet_address}' and received_timestamp >= {payment_timestamp}"
                        ))
                        rowcount = len(data)
                        if rowcount == 0:
                            if current_timestamp - int(payment_timestamp) > duration:
                                if "Alerted" in roles_str:
                                    tasks.append(asyncio.create_task(member.remove_roles(role_alerted)))
                                    tasks.append(asyncio.create_task(member.send("Your subscription to Crabalert has expired and your access to the alerts removed.")))
                                    tasks.append(asyncio.create_task(member.send("If you want to renew it, please send payments again (see #instructions for a soft reminder)")))
                            else:
                                #Send reminders if subscription is close (< 1h)
                                remaining_time = (int(payment_timestamp) + int(duration)) - current_timestamp
                                if remaining_time <= 3600*24 and not reminded:
                                    remaining_deltatime = humanize.naturaltime(current_timestamp + timedelta(seconds = remaining_time))
                                    tasks.append(asyncio.create_task(member.send(
                                        f"Your subscription to Crabalert is going to expire in less than one day (remaining:{remaining_deltatime}). If you want to keep it, please send payment again (see #instructions for a soft reminder)"
                                    )))
                                    update_query = f"UPDATE last_received_payment SET reminded='TRUE' WHERE discord_id={member.id}"
                                    execute_query(connection, update_query)
                                if "Alerted" not in roles_str:
                                    tasks.append(asyncio.create_task(member.add_roles(role_alerted)))
                        else:
                            for trans_timestamp, contract_address, txn_hash, value in data:
                                rate = 1 if contract_address.lower() in stablecoins else 1.3
                                price_coins = self._get_variable("price_coins", lambda: dict())
                                price_in_usd = price_coins.get(contract_address.lower(), -1)
                                value_in_usd = (value*price_in_usd)/rate
                                new_duration = (value_in_usd)/MONTHLY_RATE * 3600 * 24 * 30 + max((int(payment_timestamp) + int(duration)) - current_timestamp, 0)
                                trans_timestamp_date = datetime.fromtimestamp(trans_timestamp, timezone.utc)
                                update_query = f"UPDATE last_received_payment SET duration={int(round(new_duration, 0))}, received_timestamp='{current_timestamp}', reminded='FALSE' WHERE discord_id={member.id}"
                                execute_query(connection, update_query)
                                if "Alerted" not in roles_str:
                                    tasks.append(asyncio.create_task(member.add_roles(role_alerted)))
                                delta_duration = timedelta(seconds = new_duration + 3600*24*30)
                                current_timestamp_datetime = datetime.fromtimestamp(current_timestamp, timezone.utc)
                                human_friendly_duration = humanize.naturaldelta(current_timestamp_datetime - (current_timestamp_datetime+delta_duration), when=current_timestamp_datetime)
                                tasks.append(asyncio.create_task(member.send(f"Your payment of {value} {COINS_SYMBOL.get(contract_address.lower(), '???')} received at {trans_timestamp_date.strftime('%d, %b %Y')} (txn_hash : {txn_hash}) has been checked and your subscription has just been extended for a duration of {human_friendly_duration}.")))
                        
                close_database(connection)
        if tasks != []:
            asyncio.gather(*tasks)

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
        asyncio.gather(asyncio.create_task(self._refresh_prices_coin()))

    async def _refresh_prices_coin(self):
        coins_keys = [k for k in COINS.keys() if k.lower() != TUS_CONTRACT_ADDRESS.lower()]
        tasks = tuple([get_token_price_from_dexs(Web3(Web3.HTTPProvider(blockchain_urls["avalanche"])), "avalanche", c) for c in coins_keys])

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
    async def _crabada_listing_alert_loop(self):
        try:
            connection = open_database()
        except Exception as e:
            #TODO : logging
            return
        dt = datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc)
        current_timestamp = utc_time.timestamp()
        query = f"""
            SELECT * FROM crabada_listings WHERE {current_timestamp} - timestamp <= {LISTING_ITEM_EXPIRATION}
        """
        tasks = []
        data = execute_query(connection, query)
        for token_id, selling_price, timestamp, is_crab, infos_nft, infos_family in data:
            infos_nft = json.loads(infos_nft)
            if infos_family != "":
                infos_family = json.loads(infos_family)
            tasks.append(asyncio.create_task(self._notify_marketplace_item(infos_nft, infos_family, token_id, selling_price, timestamp, is_crab.lower() == "true", is_selling=False)))
        if tasks != []:
            asyncio.gather(*tasks)

    @tasks.loop(seconds=1)
    async def _crabada_selling_alert_loop(self):
        try:
            connection = open_database()
        except Exception as e:
            #TODO : logging
            return
        dt = datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc)
        current_timestamp = utc_time.timestamp()
        query = f"""
            SELECT * FROM crabada_sellings WHERE {current_timestamp} - timestamp <= {SELLING_ITEM_EXPIRATION}
        """
        tasks = []
        data = execute_query(connection, query)
        for token_id, selling_price, timestamp, is_crab, infos_nft, infos_family in data:
            infos_nft = json.loads(infos_nft)
            if infos_family != "":
                infos_family = json.loads(infos_family)
            tasks.append(asyncio.create_task(self._notify_marketplace_item(infos_nft, infos_family, token_id, selling_price, timestamp, is_crab.lower() == "true", is_selling=True)))
        if tasks != []:
            asyncio.gather(*tasks)

    async def _notify_marketplace_item(self, infos_nft, infos_family, token_id, selling_price, timestamp, is_crab, is_selling=False):
        tasks = []
        already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
        channels_to_post = (
            channel_to_post_listings_with_filters
            if not is_selling else
            channel_to_post_sellings_with_filters
        )
        for channel_id, filter_function in channels_to_post.items():
            channel = self._get_variable(f"channel_{channel_id}", f_value_if_not_exists=lambda: self.get_channel(channel_id))
            if filter_function((infos_nft, None)):
                if (token_id, timestamp, channel.id, is_selling) not in already_seen:
                    if is_crab:
                        tasks.append(asyncio.create_task(
                            self.notify_crab_item(
                                infos_nft,
                                token_id,
                                selling_price,
                                timestamp,
                                channel,
                                is_selling=is_selling
                            )
                        ))
                    else:
                        tasks.append(asyncio.create_task(
                            self.notify_egg_item(
                                infos_nft,
                                infos_family,
                                token_id,
                                selling_price,
                                timestamp,
                                channel,
                                is_selling=is_selling
                            )
                        ))
        if tasks != []:
            asyncio.gather(*tasks)
            

    async def notify_crab_item(self, infos_nft, token_id, price, timestamp_transaction, channel, is_selling=False):
        async with self._get_variable(f"sem_{CRABMESSAGE_SEM_ID}_{token_id}_{timestamp_transaction}_{channel.id}_{is_selling}", lambda: asyncio.Semaphore(value=1)):
            # print"crab from timestamp", timestamp_transaction,"will be posted", token_id, "at channel", channel.id)
            marketplace_link = f"https://marketplace.crabada.com/crabada/{token_id}"
            price_tus = self._get_variable(f"price_tus", f_value_if_not_exists=lambda:-1)
            price_usd = round(price * price_tus, 2)
            channel_id = channel.id
            tus_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("tus", ":tus:")
            crabadegg_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crabadegg", "crabadegg")
            price_formatted = "{:,.2f}".format(price)
            price_in_usd_formatted = "{:,.2f}".format(price_usd)
            tus_text = f"{tus_emoji} **{price_formatted}**"
            tus_text_len_in_space_bars = sum([1 if c == "1" or c == "." or c == "," else 2 for c in str(price)]) + 6 + 1
            usd_text = f":moneybag: **{price_in_usd_formatted}**"
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
            type_entry = "**[SOLD<aftertime>]**" if is_selling else "**[LISTING]**"
            if is_selling:
                db = open_database()
                query = f"""
                    SELECT timestamp from crabada_listings where token_id={token_id}
                """
                data = execute_query(db, query)
                duration_min = float("+inf")
                duration_argmin = None
                for ts, in data:
                    if timestamp_transaction - ts <= duration_min:
                        duration_min = timestamp_transaction - ts
                        duration_argmin = ts
                if duration_argmin is None:
                    type_entry = type_entry.replace("<aftertime>", "")
                else:
                    human_deltatime = seconds_to_pretty_print(duration_min)
                    type_entry = type_entry.replace("<aftertime>", " after "+ str(human_deltatime))
                    

                close_database(db)
            channels_to_display_shortdescrs = (
                listing_channels_to_display_shortdescrs if not is_selling else
                selling_channels_to_display_shortdescrs
            )
            buyer_seller = infos_nft['owner']
            buyer_seller_full_name = infos_nft['owner_full_name']
            if channel.id in channels_to_display_shortdescrs:
                message = (
                    f"{type_entry} :crab: {class_display}({subclass_display})\n" +
                    f"{first_column}"
                )     
            else:
                message = (
                    f"{type_entry} :crab: {'**PURE**' if infos_nft['pure_number'] == 6 else ''}{' **ORIGIN**' if infos_nft['is_origin'] == 1 else ''}{' **NO-BREED**' if infos_nft['breed_count'] == 0 else ''} {class_display}({subclass_display})\n" +
                    f"{first_column}\n" +
                    f"{second_column}\n" +
                    f"{third_column}"
                )
            asyncio.gather(asyncio.create_task(
                self._send_crab_item_message(token_id, timestamp_transaction, channel, message, marketplace_link, buyer_seller, buyer_seller_full_name, is_selling=is_selling)
            ))

    async def notify_egg_item(self, infos_nft, infos_family_nft, token_id, price, timestamp_transaction, channel, is_selling=False):
        async with self._get_variable(f"sem_{EGGMESSAGE_SEM_ID}_{token_id}_{timestamp_transaction}_{channel.id}_{is_selling}", lambda: asyncio.Semaphore(value=1)):
            type_entry = "**[SOLD<aftertime>]**" if is_selling else "**[LISTING]**"
            if is_selling:
                db = open_database()
                query = f"""
                    SELECT timestamp from crabada_listings where token_id={token_id}
                """
                data = execute_query(db, query)
                duration_min = float("+inf")
                duration_argmin = None
                for ts, in data:
                    if timestamp_transaction - ts <= duration_min:
                        duration_min = timestamp_transaction - ts
                        duration_argmin = ts
                if duration_argmin is None:
                    type_entry = type_entry.replace("<aftertime>", "")
                else:
                    human_deltatime = human_deltatime = seconds_to_pretty_print(duration_min)
                    type_entry = type_entry.replace("<aftertime>", " after "+ str(human_deltatime))
                
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
            price_formatted = "{:,.2f}".format(price)
            price_in_usd_formatted = "{:,.2f}".format(price_usd)
            tus_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("tus", ":tus:")
            tus_text = f"{tus_emoji} **{price_formatted}**"
            tus_text_len_in_space_bars = sum([1 if c == "1" or c == "." or c == "," else 2 for c in str(price)]) + 6 + 1
            usd_text = f":moneybag: **{price_in_usd_formatted}**"
            marketplace_link = f"https://marketplace.crabada.com/crabada/{token_id}"
            buyer_seller = infos_nft['owner']
            buyer_seller_full_name = infos_nft['owner_full_name']
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
            header_message = f"{type_entry} <crabadegg> {'**PURE** ' if infos_egg['probability_pure'] == 1 else ''}{egg_class_display} \n"
            footer_message = (
                ""
            )
            crab_1_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crab1", ":crab1:")#"<:crab1:934087822254694441>" if channel_id == 932591668597776414 else "<:crab_1:934075767602700288>"
            crab_2_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crab2", ":crab2:")#"<:crab2:934087853732921384>" if channel_id == 932591668597776414 else "<:crab_2:934076410132332624>"
            
            crabadegg_emoji = channels_emojis.get(channel_id, channels_emojis.get("default")).get("crabadegg", ":crabadegg:")
            channels_to_display_shortdescrs = (
                listing_channels_to_display_shortdescrs if not is_selling else
                selling_channels_to_display_shortdescrs
            )
            if channel_id in channels_to_display_shortdescrs:
                message_egg = (
                    f"{first_column}"
                )
                footer_message_egg = (
                    ""
                )
                header_message_egg = (
                    f"{type_entry} <crabadegg> {egg_class_display} \n"
                )
                    
            else:
                header_message_egg = header_message
                footer_message_egg = footer_message
                message_egg = message
            asyncio.gather(asyncio.create_task(
                self._send_egg_item_message(message_egg, header_message_egg, footer_message_egg, crab_2_emoji, tus_emoji, crab_1_emoji, crabadegg_emoji, token_id, timestamp_transaction, channel, marketplace_link, buyer_seller, buyer_seller_full_name, is_selling=is_selling)
            ))

    async def _send_crab_item_message(self, token_id, timestamp_transaction, channel, message, marketplace_link, buyer_seller, buyer_seller_full_name, is_selling=False):

        already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
        async with self._get_variable(f"semaphore_crab_message_{token_id}_{timestamp_transaction}_{channel.id}_{is_selling}", lambda: asyncio.Semaphore(value=1)):
            if (token_id, timestamp_transaction, channel.id, is_selling) not in already_seen:
                self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, channel.id, is_selling)}))
                url_buyer_seller = f"https://snowtrace.io/address/{buyer_seller}"
                embed = Embed(
                    title=marketplace_link
                )
                embed.add_field(
                    name=f"{'Bought by' if is_selling else 'Listed by'}", value=f"[{buyer_seller_full_name}]({url_buyer_seller})", inline=False
                )
                if not os.path.isfile("{token_id}.png"):
                    wget.download(f"https://photos.crabada.com/{token_id}.png", out=f"{token_id}.png", bar=None)
                
                await channel.send(message, embed=embed, file=File(f"{token_id}.png"))
                if os.path.isfile(f"{token_id}.png"):
                    os.remove(f"{token_id}.png")

    async def _send_egg_item_message(self, message_egg_in, header_message_egg, footer_message_egg, crab_2_emoji, tus_emoji, crab_1_emoji, crabadegg_emoji, token_id, timestamp_transaction, channel, marketplace_link, buyer_seller, buyer_seller_full_name, is_selling=False):
        message_egg = header_message_egg + message_egg_in + footer_message_egg
        message_egg = message_egg.replace("<crab1>", crab_1_emoji).replace("<crab2>", crab_2_emoji).replace("<tus>", tus_emoji).replace("<crabadegg>" ,crabadegg_emoji)
        already_seen = self._get_variable(f"already_seen", f_value_if_not_exists=lambda:set())
        
        async with self._get_variable(f"semaphore_egg_message_{token_id}_{timestamp_transaction}_{channel.id}_{is_selling}", lambda: asyncio.Semaphore(value=1)):
            if (token_id, timestamp_transaction, channel.id, is_selling) not in already_seen:
                self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, channel.id, is_selling)}))
                self._set_sync_variable("already_seen", already_seen.union({(token_id, timestamp_transaction, channel.id, is_selling)}))
                url_buyer_seller = f"https://snowtrace.io/address/{buyer_seller}"
                embed = Embed(
                    title=marketplace_link
                )
                embed.add_field(
                    name=f"{'Bought by' if is_selling else 'Listed by'}", value=f"[{buyer_seller_full_name}]({url_buyer_seller})", inline=False
                )
                if not os.path.isfile("egg.png"):
                    wget.download(f"https://i.ibb.co/hXcP49w/egg.png", out=f"egg.png", bar=None)
                await channel.send(message_egg, embed=embed, file=File("egg.png"))
                if os.path.isfile(f"egg.png"):
                    os.remove(f"egg.png")
