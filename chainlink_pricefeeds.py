import os
import json
import logging
import pandas_gbq
import pandas as pd
from web3 import Web3
from web3.exceptions import (InvalidEventABI, LogTopicError, MismatchedABI)
from web3._utils.events import get_event_data
from eth_utils import (encode_hex, event_abi_to_log_topic)
from google.oauth2 import service_account


# The dafault RPC from ethersjs, change it if it doesn't work: https://infura.io/docs
RPC_Endpoint = 'https://mainnet.infura.io/v3/84842078b09946638c03157f83405213'

abi = json.loads("""[
                     {"name":"decimals",
                      "inputs":[],"outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view",
                      "type":"function"},
                     {"name":"AnswerUpdated",
                      "anonymous":false,
                      "inputs":[{"indexed":true,"internalType":"int256","name":"current","type":"int256"},{"indexed":true,"internalType":"uint256","name":"roundId","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"updatedAt","type":"uint256"}],
                      "type":"event"}
                    ]
                """)

pricefeed_contracts = {
     'eth-usd' : ['ethereum', Web3.toChecksumAddress('0x37bC7498f4FF12C19678ee8fE19d713b87F6a9e6'), 0.005, 'usd', 3600]

    ,'btc-eth' : ['ethereum', Web3.toChecksumAddress('0x81076d6ff2620ea9dd7ba9c1015f0d09a3a732e6'), 0.02, 'eth', 86400]
    ,'btc-usd' : ['ethereum', Web3.toChecksumAddress('0xAe74faA92cB67A95ebCAB07358bC222e33A34dA7'), 0.005, 'usd', 3600]

    ,'usdc-eth': ['ethereum', Web3.toChecksumAddress('0xe5bbbdb2bb953371841318e1edfbf727447cef2e'), 0.01, 'eth', 86400]
    ,'usdc-usd': ['ethereum', Web3.toChecksumAddress('0x789190466E21a8b78b8027866CBBDc151542A26C'), 0.0025, 'usd', 86400]

    ,'dai-eth' : ['ethereum', Web3.toChecksumAddress('0x158228e08c52f3e2211ccbc8ec275fa93f6033fc'), 0.01, 'eth', 86400]
    ,'dai-usd' : ['ethereum', Web3.toChecksumAddress('0xDEc0a100eaD1fAa37407f0Edc76033426CF90b82'), 0.0025, 'usd', 3600]

    ,'1inch-eth': ['ethereum', Web3.toChecksumAddress('0xb2f68c82479928669b0487d1daed6ef47b63411e'), 0.02, 'eth', 86400]
    ,'1inch-usd': ['ethereum', Web3.toChecksumAddress('0xd2bdD1E01fd2F8d7d42b209c111c7b32158b5a42'), 0.01, 'usd', 86400]

    ,'aave-eth': ['ethereum', Web3.toChecksumAddress('0xdf0da6b3d19e4427852f2112d0a963d8a158e9c7'), 0.02, 'eth', 86400]
    ,'aave-usd': ['ethereum', Web3.toChecksumAddress('0xe3f0dEdE4B499c07e12475087AB1A084b5F93bc0'), 0.01, 'usd', 3600]

    ,'comp-eth': ['ethereum', Web3.toChecksumAddress('0x9d6acd34d481512586844fd65328bd358d306752'), 0.02, 'eth', 86400]
    ,'comp-usd': ['ethereum', Web3.toChecksumAddress('0x6eaC850f531d0588c0114f1E93F843B78669E6d2'), 0.01, 'usd', 3600]

    ,'dpi-eth': ['ethereum', Web3.toChecksumAddress('0x36e4f71440edf512eb410231e75b9281d4fcfc4c'), 0.02, 'eth', 86400]
    ,'dpi-usd': ['ethereum', Web3.toChecksumAddress('0x68f1b8317c19ff02fb68a8476c1d3f9fc5139c0a'), 0.01, 'usd', 3600]

    ,'fei-eth': ['ethereum', Web3.toChecksumAddress('0x4be991b4d560bba8308110ed1e0d7f8da60acf6a'), 0.02, 'eth', 86400]
    ,'fei-usd': ['ethereum', Web3.toChecksumAddress('0x1D244648d5a63618751d006886268Ae3550d0Dfd'), 0.01, 'usd', 3600]

    ,'link-eth': ['ethereum', Web3.toChecksumAddress('0xbba12740de905707251525477bad74985dec46d2'), 0.01, 'eth', 21600]
    ,'link-usd': ['ethereum', Web3.toChecksumAddress('0xDfd03BfC3465107Ce570a0397b247F546a42D0fA'), 0.01, 'usd', 3600]

    ,'snx-eth': ['ethereum', Web3.toChecksumAddress('0xbafe3cb0e563e914806a99d547bdbf2cfcf5fdf6'), 0.02, 'eth', 86400]
    ,'snx-usd': ['ethereum', Web3.toChecksumAddress('0x06ce8Be8729B6bA18dD3416E3C223a5d4DB5e755'), 0.01, 'usd', 86400]

    ,'uni-eth': ['ethereum', Web3.toChecksumAddress('0xc1d1d0da0fcf78157ea25d0e64e3be679813a1f7'), 0.02, 'eth', 86400]
    ,'uni-usd': ['ethereum', Web3.toChecksumAddress('0x68577f915131087199Fe48913d8b416b3984fd38'), 0.01, 'usd', 3600]

    ,'yfi-eth': ['ethereum', Web3.toChecksumAddress('0xaa5aa80e416f9d32ffe6c390e24410d02d203f70'), 0.01, 'eth', 86400]
    ,'yfi-usd': ['ethereum', Web3.toChecksumAddress('0x8a4D74003870064d41D4f84940550911FBfCcF04'), 0.01, 'usd', 3600]

    ,'luna-eth': ['ethereum', Web3.toChecksumAddress('0x1a8ac67a1b64f7fd71bb91c21581f036abe6aec2'), 0.02, 'eth', 86400]

    ,'ftm-eth': ['ethereum', Web3.toChecksumAddress('0xbdb80d19dea36eb7f63bdfd2bdd4033b2b7e8e4d'), 0.03, 'eth', 86400]

    ,'sushi-eth': ['ethereum', Web3.toChecksumAddress('0xd01bbb3afed2cb5ca92ca3834d441dc737f0da70'), 0.02, 'eth', 86400]
    ,'sushi-usd': ['ethereum', Web3.toChecksumAddress('0x7213536a36094cD8a768a5E45203Ec286Cba2d74'), 0.01, 'usd', 3600]

    ,'crv-eth': ['ethereum', Web3.toChecksumAddress('0x7f67ca2ce5299a67acd83d52a064c5b8e41ddb80'), 0.02, 'eth', 86400]
    ,'crv-usd': ['ethereum', Web3.toChecksumAddress('0xb4c4a493AB6356497713A78FFA6c60FB53517c63'), 0.01, 'usd', 86400]

    ,'cvx-usd': ['ethereum', Web3.toChecksumAddress('0x8d73ac44bf11cadcdc050bb2bccae8c519555f1a'), 0.02, 'usd', 86400]

    ,'ldo-eth': ['ethereum', Web3.toChecksumAddress('0x7898AcCC83587C3C55116c5230C17a6Cd9C71bad'), 0.02, 'eth', 86400]

    ,'steth-eth': ['ethereum', Web3.toChecksumAddress('0x716BB759A5f6faCdfF91F0AfB613133d510e1573'), 0.02, 'eth', 86400]
    ,'steth-usd': ['ethereum', Web3.toChecksumAddress('0xdA31bc2B08F22AE24aeD5F6EB1E71E96867BA196'), 0.01, 'usd', 3600]

    ,'frax-eth': ['ethereum', Web3.toChecksumAddress('0x56f98706C14DF5C290b02Cec491bB4c20834Bb51'), 0.02, 'eth', 86400]
    ,'frax-usd': ['ethereum', Web3.toChecksumAddress('0x61eB091ea16A32ea5B880d0b3D09d518c340D750'), 0.01, 'usd', 3600]

    ,'lusd-usd': ['ethereum', Web3.toChecksumAddress('0x27b97a63091d185cE056e1747624b9B92BAAD056'), 0.01, 'usd', 3600]

    ,'susd-eth': ['ethereum', Web3.toChecksumAddress('0x45bb69B89D60878d1e42522342fFCa9F2077dD84'), 0.01, 'eth', 86400]
    ,'susd-usd': ['ethereum', Web3.toChecksumAddress('0x1187272A0E3A603eC4734CeC73a0880055eCC593'), 0.005, 'usd', 86400]

    ,'gusd-eth': ['ethereum', Web3.toChecksumAddress('0x9c2C487DAd6C8e5bb49dC6908a29D95a234FaAd8'), 0.02, 'eth', 86400]
    ,'gusd-usd': ['ethereum', Web3.toChecksumAddress('0x6a805f2580b8D75d40331c26C074c2c42961E7F2'), 0.005, 'usd', 86400]

    ,'usdt-eth': ['ethereum', Web3.toChecksumAddress('0x7De0d6fce0C128395C488cb4Df667cdbfb35d7DE'), 0.01, 'eth', 86400]
    ,'usdt-usd': ['ethereum', Web3.toChecksumAddress('0xa964273552C1dBa201f5f000215F5BD5576e8f93'), 0.0025, 'usd', 86400]

    ,'fxs-usd': ['ethereum', Web3.toChecksumAddress('0x9d78092775DFE715dFe1b0d71aC1a4d6e3652559'), 0.02, 'usd', 86400]

    ,'spell-usd': ['ethereum', Web3.toChecksumAddress('0x8640b23468815902e011948F3aB173E1E83f9879'), 0.01, 'usd', 86400]

    ,'avax-usd': ['ethereum', Web3.toChecksumAddress('0x0fC3657899693648bbA4dbd2d8b33b82E875105D'), 0.02, 'usd', 86400]

    ,'mim-usd': ['ethereum', Web3.toChecksumAddress('0x18f0112E30769961AF90FDEe0D1c6B27E6d72D92'), 0.005, 'usd', 86400]
    }

config = {'ethereum':pricefeed_contracts,
         }

def get_event_abi(abi, abi_name, abi_type = 'event'):
    l = [x for x in abi if x['type'] == abi_type and x['name']==abi_name]
    return l[0]
def get_logs (w3, contract_address, topics, events, from_block=0, to_block=None):

    if not to_block:
        to_block = w3.eth.get_block('latest').number

    try:
        logs = w3.eth.get_logs({"address": contract_address
                                ,"topics":topics
                                ,"fromBlock": from_block
                                ,"toBlock": to_block
                                })
    except ValueError:
        logs = None
        from_block = 12*10**6
        batch_size =  3*10**5
        while from_block < to_block:
            logging.info(f'from_block {from_block} to_block {from_block + batch_size}')
            batch_logs = w3.eth.get_logs({"address": contract_address
                                    ,"topics":topics
                                    ,"fromBlock": from_block
                                    ,"toBlock": from_block + batch_size # narrow down the range to avoid too many results error
                                    })
            if not logs:
                logs = batch_logs
            else:
                logs = logs + batch_logs
            if len(logs) > 0:
                logging.info(f'Total logs count {len(logs)}')

            from_block = from_block + batch_size + 1

    all_events = []
    for l in logs:
        try:
            evt_topic0 = l['topics'][0].hex()
            evt_abi = [x for x in events if encode_hex(event_abi_to_log_topic(x)) == evt_topic0][0]
            evt = get_event_data(w3.codec, evt_abi, l)
        except MismatchedABI: #if for some reason there are other events
            pass
        all_events.append(evt)
    df = pd.DataFrame(all_events)
    return df

def main():
    gcp_project_id = 'gearbox-336415'
    from_block = 0
    try:
        df_blocknum = pandas_gbq.read_gbq('select max(blockNumber) from gearbox.oracle_price_history',
                                 project_id=gcp_project_id,
                                 progress_bar_type = None,)
        from_block = int(df_blocknum.iloc[0,0]) + 1
    except pandas_gbq.exceptions.GenericGBQException:
        logging.info('The table does not exist?')
        from_block = 0

    #from_block = 0
    logging.info(f'from_block={from_block}')

    df_columns = ['ticker', 'updated_at', 'chain','price', 'price_decimal', 'type', 'threshold', 'base', 'heartbeat', 'decimals', 'blockNumber','tx_hash']
    df = pd.DataFrame(columns = df_columns)

    event_AnswerUpdated = get_event_abi(abi, 'AnswerUpdated')
    topic_AnswerUpdated  = encode_hex(event_abi_to_log_topic(event_AnswerUpdated))

    for chain in config.keys():
        chain_contracts = config[chain]
        for ticker in chain_contracts.keys():
            w3 = w3_connection[chain_contracts[ticker][0]]
            address = chain_contracts[ticker][1]
            threshold = chain_contracts[ticker][2]
            base = chain_contracts[ticker][3]
            heartbeat = chain_contracts[ticker][4]

            decimals = w3.eth.contract(address=address, abi=abi).functions.decimals().call()

            df_ticker = get_logs(w3, address, [topic_AnswerUpdated], [event_AnswerUpdated], from_block)
            if len(df_ticker)>0:
                df_ticker[['chain', 'ticker', 'threshold', 'base', 'heartbeat', 'decimals', 'type']] = [chain , ticker, threshold, base, heartbeat, decimals, 'direct']

                df_ticker['tx_hash']       = df_ticker['transactionHash'].apply(lambda x: x.hex())
                df_ticker['updated_at']    = df_ticker['args'].apply(lambda x: pd.to_datetime(x['updatedAt'], unit='s'))
                df_ticker['price']         = df_ticker['args'].apply(lambda x: x['current'])
                df_ticker['price_decimal'] = df_ticker['price']/10**(decimals)

                df = df.append(df_ticker[df_columns], ignore_index=True)
                logging.info(f'''{ticker.upper()}: observation count = {len(df_ticker)}, price range: {df_ticker['price_decimal'].min()}-{df_ticker['price_decimal'].max()}''')

                if ticker == 'eth-usd':
                    df_eth_usd = df_ticker
                elif base == 'usd': # usd2eth cross rate
                    ticker = ticker.replace('-usd','-eth')
                    if ticker not in chain_contracts.keys():
                        df_cross = pd.DataFrame(columns = df_columns)
                        for k, t in df_ticker[df_columns].iterrows():
                            e = df_eth_usd[df_eth_usd['updated_at'] <= t['updated_at']]
                            if len(e) > 0:
                                i = (t['updated_at'] - e['updated_at']).idxmin()
                                e_price_decimal, e_decimals = e.loc[i][['price_decimal','decimals']]
                                t.ticker = ticker
                                t.price_decimal = t.price_decimal/e_price_decimal
                                t.decimals = 18
                                t.base = 'eth'
                                t.type = 'cross'
                                t.price = int(t.price_decimal*10**(t.decimals))
                                df_cross = df_cross.append(t)
                        df = df.append(df_cross, ignore_index=True)
                        logging.info(f'''cross rate {ticker.upper()}: observation count = {len(df_cross)}, price range: {df_cross['price_decimal'].min()}-{df_cross['price_decimal'].max()}''')
                elif base == 'eth': #eth2usd cross rate
                    ticker = ticker.replace('-eth','-usd')
                    if ticker not in chain_contracts.keys():
                        df_cross = pd.DataFrame(columns = df_columns)
                        for k, t in df_ticker[df_columns].iterrows():
                            e = df_eth_usd[df_eth_usd['updated_at'] <= t['updated_at']]
                            if len(e) > 0:
                                i = (t['updated_at'] - e['updated_at']).idxmin()
                                e_price_decimal, e_decimals = e.loc[i][['price_decimal','decimals']]
                                t.ticker = ticker
                                t.decimals = e_decimals
                                t.base = 'usd'
                                t.type = 'cross'
                                t.price_decimal = t.price_decimal*e_price_decimal
                                t.price = int(t.price_decimal*10**(t.decimals))
                                df_cross = df_cross.append(t)
                        df = df.append(df_cross, ignore_index=True)
                        logging.info(f'''cross rate {ticker.upper()}: observation count = {len(df_cross)}, price range: {df_cross['price_decimal'].min()}-{df_cross['price_decimal'].max()}''')

    df = df.sort_values(by = 'updated_at').reset_index(drop=True)
    pd.set_option("precision", 18)

    logging.info(df)

    credentials = service_account.Credentials.from_service_account_file(
        'gearbox-336415-5ed144668529.json',
    )
    gcp_project_id = 'gearbox-336415'
    if len(df)>0:
        numeric_cols = ['price', 'price_decimal','heartbeat','decimals','blockNumber']
        df[numeric_cols] = df[numeric_cols].astype('float64')

        pandas_gbq.to_gbq(df,
                          'gearbox.oracle_price_history',
                          project_id=gcp_project_id,
                          if_exists = ('replace' if from_block==0 else 'append'),
                          progress_bar = False)
        logging.info('gearbox.oracle_price_history, insert done')
    else:
        logging.info('No new events since block', from_block)

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(funcName)s %(levelname)s %(message)s',
        level=os.environ.get('LOGLEVEL', 'INFO').upper(),
        datefmt='%Y-%m-%d %H:%M:a%S',
        )
    w3_connection = {'ethereum' : Web3(Web3.HTTPProvider(RPC_Endpoint, request_kwargs={'timeout': 30}))}
    logging.info (f'Ethereum connected: {w3_connection["ethereum"].isConnected()}')

    main()
