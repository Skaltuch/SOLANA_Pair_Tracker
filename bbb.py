import asyncio
import websockets
import json
from solana.rpc.api import Client
from solders.pubkey import Pubkey
from solders.signature import Signature
import pandas as pd
from tabulate import tabulate
import telebot
import requests
from concurrent.futures import ThreadPoolExecutor
import logging
from websockets.exceptions import WebSocketException

# SETUP
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

wallet_address = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
seen_signatures = set()
solana_client = Client("https://api.mainnet-beta.solana.com")


bot = telebot.TeleBot("TELEGRAM BOT TOKEN")

new_pairs = []

def get_token_info(token_address):
    try:
        url = "https://api.mainnet-beta.solana.com"
        headers = {"Content-Type": "application/json"}


        supply_data = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenSupply",
            "params": [token_address]
        }


        metadata_data = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                token_address,
                {"encoding": "jsonParsed"}
            ]
        }

        with ThreadPoolExecutor(max_workers=2) as executor:
            supply_future = executor.submit(requests.post, url, headers=headers, json=supply_data)
            metadata_future = executor.submit(requests.post, url, headers=headers, json=metadata_data)

            supply_response = supply_future.result()
            metadata_response = metadata_future.result()

        supply_result = supply_response.json()['result']['value']
        supply = float(supply_result['amount']) / (10 ** supply_result['decimals'])

        metadata_result = metadata_response.json()['result']['value']
        if metadata_result and 'data' in metadata_result:
            parsed_data = metadata_result['data']['parsed']['info']
            token_name = parsed_data.get('name', 'Unknown')
            token_symbol = parsed_data.get('symbol', 'Unknown')
        else:
            token_name = 'ZAB 1'
            token_symbol = '(Y)'

        return f"{token_name} ({token_symbol})", supply
    except Exception as e:
        logging.error(f"Error fetching token info for {token_address}: {e}")
        return "Unknown", "Unknown"

def getTokens(str_signature):
    try:
        signature = Signature.from_string(str_signature)
        transaction = solana_client.get_transaction(signature, encoding="jsonParsed",
                                                    max_supported_transaction_version=0).value
        instruction_list = transaction.transaction.transaction.message.instructions
        if any("initialize2" in message for message in transaction.transaction.meta.log_messages):
            for instructions in instruction_list:
                if instructions.program_id == Pubkey.from_string(wallet_address):
                    logging.info("New Pairs detected")
                    Token0 = str(instructions.accounts[8])
                    Token1 = str(instructions.accounts[9])

                    Token0_info, Token0_supply = get_token_info(Token0)
                    Token1_info, Token1_supply = get_token_info(Token1)


                    data = {
                        'Token_Index': ['Token0', 'Token1'],
                        'Account Public Key': [Token0, Token1],
                        'Token Info': [Token0_info, Token1_info],
                        'Token Supply': [Token0_supply, Token1_supply]
                    }

                    df = pd.DataFrame(data)
                    table = tabulate(df, headers='keys', tablefmt='fancy_grid')
                    logging.info(f"\n{table}")


                    new_pair_info = (
                        f"New Pool Detected:\n"
                        f"Token0: {Token0_info}\n"
                        f"Address: {Token0}\n"
                        f"Token1: {Token1_info}\n"
                        f"Address: {Token1}\n"
                    )

                    new_pairs.append(new_pair_info)
                    return Token0_info, Token0_supply, Token1_info, Token1_supply

        return None, None, None, None
    except Exception as e:
        logging.error(f"Error in getTokens: {e}")
        return None, None, None, None

async def connect_websocket():
    uri = "wss://api.mainnet-beta.solana.com"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                logging.info("Connected to WebSocket")
                await websocket.send(json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [wallet_address]},
                        {"commitment": "finalized"}
                    ]
                }))

                first_resp = await websocket.recv()
                response_dict = json.loads(first_resp)
                if 'result' in response_dict:
                    logging.info(f"Subscription ID: {response_dict['result']}")

                while True:
                    try:
                        response = await websocket.recv()
                        await process_message(response)
                    except websockets.exceptions.ConnectionClosed:
                        logging.warning("WebSocket connection closed. Reconnecting...")
                        break
        except Exception as e:
            logging.error(f"WebSocket connection error: {e}")
            await asyncio.sleep(5)

async def process_message(response):
    response_dict = json.loads(response)

    if 'params' in response_dict and 'result' in response_dict['params']:
        result = response_dict['params']['result']
        if 'value' in result and result['value'].get('err') is None:
            signature = result['value']['signature']

            if signature not in seen_signatures:
                seen_signatures.add(signature)
                log_messages_set = set(result['value']['logs'])

                search = "initialize2"
                if any(search in message for message in log_messages_set):
                    logging.info(f"New pair initialization detected: {signature}")
                    Token0, Token0_supply, Token1, Token1_supply = getTokens(signature)
                    if Token0 and Token1:
                        new_pair_info = f"New pair detected:\nToken0: {Token0} (Supply: {Token0_supply})\nToken1: {Token1} (Supply: {Token1_supply})"
                        new_pairs.append(new_pair_info)

@bot.message_handler(commands=['new'])
def send_new_pairs(message):
    global new_pairs
    if not new_pairs:
        bot.reply_to(message, "No new pairs detected since the last check.")
    else:
        for pair in new_pairs:
            bot.send_message(message.chat.id, pair)
        new_pairs = []

async def main():
    await connect_websocket()

if __name__ == "__main__":

    import threading

    bot_thread = threading.Thread(target=bot.polling, daemon=True)
    bot_thread.start()

    asyncio.run(main())