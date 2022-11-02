import time
import os
from datetime import datetime
from pprint import pformat
import logging
import requests
import json
import pandas as pd
from web3 import Web3
from web3.exceptions import (ContractLogicError, InvalidEventABI, ABIFunctionNotFound, LogTopicError, MismatchedABI)
from web3._utils.events import get_event_data
from eth_utils import (encode_hex, event_abi_to_log_topic)
from web3.datastructures import AttributeDict
from multicall import Call, Multicall
from google.oauth2 import service_account
import pandas_gbq

Etherscan_APIKEY   = None #optional, but recommended
GearboxAddressProvider = '0xcF64698AFF7E5f27A11dff868AF228653ba53be0'

# The dafault RPC from ethersjs, change it if it doesn't work: https://infura.io/docs
RPC_Endpoint = 'https://mainnet.infura.io/v3/84842078b09946638c03157f83405213'

def parse_abi(abi_dict, abi_type = None):
    recs = []
    for contract_type in abi_dict.keys():
        for rec in [x for x in abi_dict[contract_type] if not abi_type or x['type'] == abi_type]:
            topic = None
            if rec['type'] == 'event':
                topic = encode_hex(event_abi_to_log_topic(rec))
            name = None
            if 'name' in rec:
                name = rec['name']

            recs.append({'contract_type': contract_type,
                           'name'        : name,
                           'type'        : rec['type'],
                           'abi'         : rec,
                           'topic'       : topic,
                           })

    df = pd.DataFrame(recs)
    return df
def get_logs (w3, contract_address, df_abi, topics=None, from_block=0, to_block='latest'):
    logs = w3.eth.get_logs({"address": contract_address
                           ,"topics":topics
                           ,"fromBlock": from_block
                           ,"toBlock": to_block
                           })

    all_events = []
    for l in logs:
        try:
            evt_topic0 = l['topics'][0].hex()
            evt_abi = df_abi[df_abi['topic']== evt_topic0]['abi'].values[0]
            evt = get_event_data(w3.codec, evt_abi, l)
        except MismatchedABI: #if for some reason there are other events
            pass
        all_events.append(evt)
    df = pd.DataFrame(all_events)
    return df
def pull_abi_etherscan(contract_address, apikey = Etherscan_APIKEY):
    url = 'https://api.etherscan.io/api?module=contract&action=getabi'
    params = {'address':contract_address,'apikey' : apikey}

    if not apikey:
        logging.info('etherscan apikey is not set: 5 sec wait due to rate-limit')
        time.sleep(5) # rate-limit when apikey is empty

    response = requests.get(url, params=params)
    response_json = json.loads(response.text)

    if response_json['status']  == '1':
        return json.loads(response_json['result'])
    else:
        raise Exception(response_json['result'])

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
def combine_description(description):
    text_description = ''
    for x in description:
        if x['type']=='tuple':
            text_description += '(' + combine_description(x['components'])+')'+','
        elif x['type']=='tuple[]':
            text_description += '(' + combine_description(x['components']) + ')[]' +','

        else:
            text_description += x['internalType']+','
    return text_description[0:-1]

def get_function_signature(function_name, df_abi, inputs=None, outputs=None):
    if not inputs:
        inputs = df_abi[df_abi['name']==function_name]['abi'].values[0]['inputs']
    if not outputs:
        outputs= df_abi[df_abi['name']==function_name]['abi'].values[0]['outputs']

    return function_name +'(' + combine_description(inputs) + ')' + '(' + combine_description(outputs) +')'

def get_data_multicall(w3, df, function_name, df_abi, contract_address = None):

    function_signature = get_function_signature(function_name, df_abi )

    if function_name == 'creditAccounts':
        multi_result = Multicall([
                    Call(contract_address, [function_signature, x], [[x, Web3.toChecksumAddress]]) for x in df['id']
                    ]
                    ,_w3 = w3)
    elif function_name == 'creditManager':
        multi_result = Multicall([
                        Call(y, [function_signature], [[x, Web3.toChecksumAddress]]) for x,y in zip(df['id'],df['CA'])
                        ]
                        ,_w3 = w3)
    elif function_name == 'borrowedAmount':
        multi_result = Multicall([
                        Call(y, [function_signature], [[x, None]]) for x,y in zip(df['id'],df['CA'])
                        ]
                        ,_w3 = w3)
    elif function_name == 'since':
        multi_result = Multicall([
                        Call(y, [function_signature], [[x, None]]) for x,y in zip(df['id'],df['CA'])
                        ]
                        ,_w3 = w3)
    elif function_name == 'getCreditAccountData':
        multi_result = Multicall([
                        Call(contract_address, [function_signature, y, z], [[x, None]]) for x,y,z in zip(df['id'],df['CM'],df['Borrower'])
                        ]
                        ,_w3 = w3)
    try:
        multi_result = multi_result()
    except (requests.exceptions.HTTPError):
        time.sleep(3)
        multi_result = multi_result()

    d_multi_result = AttributeDict.recursive(multi_result)
    return d_multi_result

def getCreditAccountData(id, cm, borrower):
    try:
        return w3_eth.eth.contract(address=DataCompressor, abi=DataCompressor_abi).functions.getCreditAccountData(cm, borrower).call()
    except:
        logging.error('Error: id=',id,'CM=',cm,'Borrower=',borrower)


def get_token_balance(df_row, token, data_cols, d_data, allowedTokens):
    ret = 0
    try:
        if d_data[df_row['id']]:
            ret = {allowedTokens[Web3.toChecksumAddress(d[0])]['symbol']:d[1] for d in list({y:z for y, z in zip(data_cols, d_data[df_row['id']])}['balances'])}[token]
    except KeyError:
        pass
    return ret

def main():
    logging.info('Start')
    AddressProvider = Web3.toChecksumAddress(GearboxAddressProvider)
    AddressProvider_abi = pull_abi_etherscan(AddressProvider)

    AccountFactory = w3_eth.eth.contract(address=AddressProvider, abi=AddressProvider_abi).functions.getAccountFactory().call()
    logging.info(f'AccountFactory: {AccountFactory}')
    AccountFactory_abi = pull_abi_etherscan(AccountFactory)
    countCreditAccounts = w3_eth.eth.contract(address=AccountFactory, abi=AccountFactory_abi).functions.countCreditAccounts().call()
    countCreditAccountsInStock = w3_eth.eth.contract(address=AccountFactory, abi=AccountFactory_abi).functions.countCreditAccountsInStock().call()
    logging.info(f'countCreditAccounts: {countCreditAccounts}')
    logging.info(f'countCreditAccountsInStock: {countCreditAccountsInStock}')
    masterCreditAccount = w3_eth.eth.contract(address=AccountFactory, abi=AccountFactory_abi).functions.masterCreditAccount().call()
    logging.info(f'masterCreditAccount: {masterCreditAccount}')
    CreditAccount_abi = pull_abi_etherscan(masterCreditAccount)

    DataCompressor = w3_eth.eth.contract(address=AddressProvider, abi=AddressProvider_abi).functions.getDataCompressor().call()
    logging.info(f'DataCompressor: {DataCompressor}')
    DataCompressor_abi = pull_abi_etherscan(DataCompressor)

    ContractsRegister = w3_eth.eth.contract(address=AddressProvider, abi=AddressProvider_abi).functions.getContractsRegister().call()
    logging.info(f'ContractsRegister: {ContractsRegister}')
    ContractsRegister_abi = pull_abi_etherscan(ContractsRegister)

    CreditManagers = w3_eth.eth.contract(address=ContractsRegister, abi=ContractsRegister_abi).functions.getCreditManagers().call()
    logging.info(f'CreditManagers: {CreditManagers}')
    version = [None] * len(CreditManagers)

    cm_dict = {AccountFactory: {'token': None, 'symbol': None, 'decimals': None, 'version': None,'creditFacade': None}}
    allowedTokens = {}
    CreditManager_abi = Token_abi = CreditFilter_abi = creditFacade_abi = priceOracle_abi = ''

    for i, CreditManager in enumerate(CreditManagers):
        logging.info(f'CreditManager {i+1} of {len(CreditManagers)}')

        if not CreditManager_abi:
            CreditManager_abi = pull_abi_etherscan(CreditManager)

        version[i] = w3_eth.eth.contract(address=CreditManager, abi=CreditManager_abi).functions.version().call()

        if i>0 and version[i]!=version[i-1]:
            CreditManager_abi = pull_abi_etherscan(CreditManager)

        logging.info(f'version {version[i]}')


        if version[i] == 1:
            Token = w3_eth.eth.contract(address=CreditManager, abi=CreditManager_abi).functions.underlyingToken().call()
            CreditFilter = w3_eth.eth.contract(address=CreditManager, abi=CreditManager_abi).functions.creditFilter().call()
            if not CreditFilter_abi:
                CreditFilter_abi = pull_abi_etherscan(CreditFilter)
            allowedTokensCount = w3_eth.eth.contract(address=CreditFilter, abi=CreditFilter_abi).functions.allowedTokensCount().call()
            priceOracle        = w3_eth.eth.contract(address=CreditFilter, abi=CreditFilter_abi).functions.priceOracle().call()
            wethAddress        = w3_eth.eth.contract(address=CreditFilter, abi=CreditFilter_abi).functions.wethAddress().call()
            CreditManager_v1_abi = CreditManager_abi
            creditFacade = None
        else:
            Token = w3_eth.eth.contract(address=CreditManager, abi=CreditManager_abi).functions.underlying().call()
            allowedTokensCount = w3_eth.eth.contract(address=CreditManager, abi=CreditManager_abi).functions.collateralTokensCount().call()
            priceOracle        = w3_eth.eth.contract(address=CreditManager, abi=CreditManager_abi).functions.priceOracle().call()
            wethAddress        = w3_eth.eth.contract(address=CreditManager, abi=CreditManager_abi).functions.wethAddress().call()

            creditFacade = w3_eth.eth.contract(address=CreditManager, abi=CreditManager_abi).functions.creditFacade().call()
            if not creditFacade_abi:
                creditFacade_abi = pull_abi_etherscan(creditFacade)

        if not Token_abi:
            Token_abi = pull_abi_etherscan(Token)

        if not priceOracle_abi or version[i]!=version[i-1]:
            priceOracle_abi = pull_abi_etherscan(priceOracle)

        Token_symbol = w3_eth.eth.contract(address=Token, abi=Token_abi).functions.symbol().call()
        Token_decimals = w3_eth.eth.contract(address=Token, abi=Token_abi).functions.decimals().call()
        cm_dict[CreditManager] = {'version' : version[i],
                                  'creditFacade': creditFacade,
                                  'token': Token,
                                  'symbol': Token_symbol,
                                  'decimals': Token_decimals,
                                  'priceOracle' : priceOracle,
                                  'allowedTokensCount': allowedTokensCount,
                                  'allowedTokens':{},
                                 }
        for token_id in range(allowedTokensCount):
            if version[i] == 1:
                allowed_token = w3_eth.eth.contract(address=CreditFilter, abi=CreditFilter_abi).functions.allowedTokens(token_id).call()
            else:
                allowed_token = w3_eth.eth.contract(address=CreditManager, abi=CreditManager_abi).functions.collateralTokens(token_id).call()
                allowed_token = allowed_token[0]

            allowed_token_symbol = w3_eth.eth.contract(address=allowed_token, abi=Token_abi).functions.symbol().call()
            allowed_token_decimals = w3_eth.eth.contract(address=allowed_token, abi=Token_abi).functions.decimals().call()



            if version[i] == 1:
                #try:
                #    allowed_token_weth_priceOracle = w3_eth.eth.contract(address=priceOracle, abi=priceOracle_abi).functions.getLastPrice(allowed_token, wethAddress).call()
                #    allowed_token_weth_priceOracle = allowed_token_weth_priceOracle*(10**allowed_token_decimals)/1e18
                #except ContractLogicError:
                #    allowed_token_weth_priceOracle = None
                #    logging.info(allowed_token_symbol+'-WETH getLastPrice error')
                #try:
                #    allowed_token_underlying_priceOracle = w3_eth.eth.contract(address=priceOracle, abi=priceOracle_abi).functions.getLastPrice(allowed_token, Token).call()
                #    allowed_token_underlying_priceOracle = allowed_token_underlying_priceOracle*(10**allowed_token_decimals)/(10**Token_decimals)
                #except ContractLogicError:
                #    allowed_token_underlying_priceOracle = None
                #    logging.info(allowed_token_symbol+'-underlying getLastPrice error')
                USDC_Address = '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
                allowed_token_usd_priceOracle = w3_eth.eth.contract(address=priceOracle, abi=priceOracle_abi).functions.getLastPrice(allowed_token, USDC_Address).call()/1e18
            else:
                try:
                    #allowed_token_weth_priceOracle = w3_eth.eth.contract(address=priceOracle, abi=priceOracle_abi).functions.convert(10**8, allowed_token, wethAddress).call()
                    #allowed_token_weth_priceOracle = allowed_token_weth_priceOracle*10**(-8)
                    #allowed_token_underlying_priceOracle = w3_eth.eth.contract(address=priceOracle, abi=priceOracle_abi).functions.convert(10**8, allowed_token, Token).call()
                    #allowed_token_underlying_priceOracle = allowed_token_underlying_priceOracle*10**(-8)

                    allowed_token_usd_priceOracle = w3_eth.eth.contract(address=priceOracle, abi=priceOracle_abi).functions.getPrice(allowed_token).call()
                    allowed_token_usd_priceOracle=allowed_token_usd_priceOracle*10**(-8)
                except ContractLogicError:
                    #allowed_token_usd_priceOracle = allowed_token_underlying_priceOracle= None
                    allowed_token_usd_priceOracle = None
                    logging.info(allowed_token_symbol+'- getPrice error')

            Token_dict={'symbol'          : allowed_token_symbol,
                        'decimals'        : allowed_token_decimals,
                        'Price_USD'       : allowed_token_usd_priceOracle,
                        #'Price_WETH'      : allowed_token_weth_priceOracle,
                        #'Price_'+cm_dict[CreditManager]['symbol']: allowed_token_underlying_priceOracle,
                        }

            if allowed_token in allowedTokens:
                allowedTokens[allowed_token].update(Token_dict)
            else:
                allowedTokens[allowed_token] = Token_dict

    df_abi = parse_abi({'AddressProvider':AddressProvider_abi,
                        'AccountFactory': AccountFactory_abi,
                        'DataCompressor': DataCompressor_abi,
                        'CreditAccount': CreditAccount_abi,
                        'CreditManager': CreditManager_abi,
                        'creditFacade': creditFacade_abi,
                        'CreditManager_v1': CreditManager_v1_abi,
                       })

    logging.info(pformat(cm_dict))
    logging.info(pformat(allowedTokens))

    df = pd.DataFrame()
    df['id'] = range(countCreditAccounts)
    for ids in list(chunks(list(df['id']), 1000)): #chunk size for multicall = 1000 (reduce in case of issues)
        id_range = list(df['id'].isin(ids))
        d_ca = get_data_multicall(w3_eth, df.loc[id_range], 'creditAccounts', df_abi[(df_abi['contract_type']=='AccountFactory')], AccountFactory)
        df.loc[id_range,'CA'] = df.loc[id_range].apply(lambda x: d_ca[x['id']], axis=1)

        d_cm = get_data_multicall(w3_eth, df.loc[id_range], 'creditManager', df_abi[(df_abi['contract_type']=='CreditAccount')])
        df.loc[id_range,'CM'] = df.loc[id_range].apply(lambda x: d_cm[x['id']], axis=1)

        d_amount = get_data_multicall(w3_eth, df.loc[id_range], 'borrowedAmount', df_abi[(df_abi['contract_type']=='CreditAccount')])
        df.loc[id_range,'borrowedAmount'] = df.loc[id_range].apply(lambda x: d_amount[x['id']] if d_amount[x['id']] > 1 else 0 , axis=1)

        d_since = get_data_multicall(w3_eth, df.loc[id_range], 'since', df_abi)
        df.loc[id_range,'Since'] = df.loc[id_range].apply(lambda x: d_since[x['id']], axis=1)

    df['Since'] = df['Since'].astype(int)
    df['Decimals'] = df.apply(lambda x: cm_dict[x['CM']]['decimals'], axis=1)
    df['Symbol'] = df.apply(lambda x: cm_dict[x['CM']]['symbol'] , axis=1)
    df['CF'] = df.apply(lambda x: cm_dict[x['CM']]['creditFacade'] , axis=1)

    logging.info(df)

    #Open, Close, Repay, Liquidate
    OpenCreditAccount_topic             =  df_abi[(df_abi['name']=='OpenCreditAccount') &(df_abi['contract_type']=='creditFacade') & (df_abi['type']=='event')]['topic'].values[0]
    OpenCreditAccount_topic_v1          =  df_abi[(df_abi['name']=='OpenCreditAccount') &(df_abi['contract_type']=='CreditManager_v1') & (df_abi['type']=='event')]['topic'].values[0]
    AddCollateral_topic                 =  df_abi[(df_abi['name']=='AddCollateral') & (df_abi['contract_type']=='creditFacade') & (df_abi['type']=='event')]['topic'].values[0]
    AddCollateral_topic_v1              =  df_abi[(df_abi['name']=='AddCollateral') & (df_abi['contract_type']=='CreditManager_v1') & (df_abi['type']=='event')]['topic'].values[0]
    CloseCreditAccount_topic            =  df_abi[(df_abi['name']=='CloseCreditAccount') & (df_abi['contract_type']=='creditFacade') & (df_abi['type']=='event')]['topic'].values[0]
    CloseCreditAccount_topic_v1         =  df_abi[(df_abi['name']=='CloseCreditAccount') & (df_abi['contract_type']=='CreditManager_v1') & (df_abi['type']=='event')]['topic'].values[0]
    RepayCreditAccount_topic_v1         =  df_abi[(df_abi['name']=='RepayCreditAccount') & (df_abi['contract_type']=='CreditManager_v1') & (df_abi['type']=='event')]['topic'].values[0]
    LiquidateCreditAccount_topic        =  df_abi[(df_abi['name']=='LiquidateCreditAccount') & (df_abi['contract_type']=='creditFacade') & (df_abi['type']=='event')]['topic'].values[0]
    LiquidateCreditAccount_topic_v1     =  df_abi[(df_abi['name']=='LiquidateCreditAccount') & (df_abi['contract_type']=='CreditManager_v1') & (df_abi['type']=='event')]['topic'].values[0]
    LiquidateExpiredCreditAccount_topic =  df_abi[(df_abi['name']=='LiquidateExpiredCreditAccount')  & (df_abi['contract_type']=='creditFacade') &(df_abi['type']=='event')]['topic'].values[0]

    logging.info(f'OpenCreditAccount_topics: {OpenCreditAccount_topic_v1}, {OpenCreditAccount_topic}' )
    logging.info(f'CloseCreditAccount_topic: {CloseCreditAccount_topic_v1}, {CloseCreditAccount_topic}')
    logging.info(f'RepayCreditAccount_topic: {RepayCreditAccount_topic_v1}')
    logging.info(f'LiquidateCreditAccount_topic: {LiquidateCreditAccount_topic_v1}, {LiquidateCreditAccount_topic}')
    logging.info(f'LiquidateExpiredCreditAccount_topic: {LiquidateExpiredCreditAccount_topic}')

    logs = pd.DataFrame()
    for CM in CreditManagers:

        if len(df.loc[df['CM']==CM]['Since'])>0:
            block_from = df.loc[df['CM']==CM]['Since'].min()

            CM_logs = get_logs(w3_eth, CM, df_abi,
                               [[OpenCreditAccount_topic_v1,
                                 AddCollateral_topic_v1,
                                 CloseCreditAccount_topic_v1,
                                 RepayCreditAccount_topic_v1,
                                 LiquidateCreditAccount_topic_v1,]
                                 ],
                                 df.loc[df['CM']==CM]['Since'].min(),
                                 'latest')
            logs = logs.append(CM_logs, ignore_index = True)

            if cm_dict[CM]['creditFacade']:
                CF_logs = get_logs(w3_eth, cm_dict[CM]['creditFacade'], df_abi,
                                   [[OpenCreditAccount_topic,
                                     AddCollateral_topic,
                                     CloseCreditAccount_topic,
                                     LiquidateCreditAccount_topic,
                                     LiquidateExpiredCreditAccount_topic,]
                                     ],
                                     df.loc[df['CM']==CM]['Since'].min(),
                                     'latest')
                logs = logs.append(CF_logs, ignore_index = True)

    i=0
    for row in df.loc[df['CM'].isin(CreditManagers)].itertuples():
        i+=1
        open_events       = logs[(logs['address'].isin([row.CM,row.CF])) & (logs['blockNumber']>=row.Since) & (logs['event']=='OpenCreditAccount')]['args'].values
        collateral_events = logs[(logs['address'].isin([row.CM,row.CF])) & (logs['blockNumber']>=row.Since) & (logs['event']=='AddCollateral')]['args'].values
        close_events      = logs[(logs['address'].isin([row.CM,row.CF])) & (logs['blockNumber']>=row.Since) & (logs['event']=='CloseCreditAccount')]['args'].values
        repay_events      = logs[(logs['address'].isin([row.CM,row.CF])) & (logs['blockNumber']>=row.Since) & (logs['event']=='RepayCreditAccount')]['args'].values
        liquidate_event   = logs[(logs['address'].isin([row.CM,row.CF])) & (logs['blockNumber']>=row.Since) & (logs['event'].isin(['LiquidateCreditAccount', 'LiquidateExpiredCreditAccount']))]['args'].values




        CA_open_event = [x for x in open_events if x['creditAccount']== row.CA] # Open
        if len(CA_open_event) > 0:
            Borrower = CA_open_event[0]['onBehalfOf']
            df.loc[df['id']==row.id, 'Borrower'] = Borrower

            Collateral = sum([x['amount'] for x in CA_open_event if 'amount' in x])
            CA_collateral_event = [x for x in collateral_events if x['onBehalfOf']== Borrower] # Open
            if len(CA_collateral_event) > 0:
                Collateral = Collateral + sum(
                                            [(x['value']*(allowedTokens[x['token']]['Price_USD'])*(10**-allowedTokens[x['token']]['decimals']))
                                             /(allowedTokens[cm_dict[row.CM]['token']]['Price_USD']*(10**-row.Decimals))
                                            for x in CA_collateral_event
                                            if allowedTokens[cm_dict[row.CM]['token']]['Price_USD']>0
                                            ]
                                         )
            df.loc[df['id']==row.id, 'Collateral'] = Collateral

            CA_close_event = [x for x in close_events if x['to']== Borrower] # Close
            if len(CA_close_event) > 0:
                df.loc[df['id']==row.id, ['Borrower', 'borrowedAmount', 'Collateral']] = [None, 0, 0]

            CA_repay_event = [x for x in repay_events if x['to']== Borrower] # Repay
            if len(CA_repay_event) > 0:
                df.loc[df['id']==row.id, ['Borrower', 'borrowedAmount', 'Collateral']] = [None, 0, 0]

            CA_liquidate_event = [x for x in liquidate_event if x['owner']== Borrower] # Liquidate
            if len(CA_liquidate_event) > 0:
                df.loc[df['id']==row.id, ['Borrower', 'borrowedAmount', 'Collateral']] = [None, 0, 0]

        if i % 50 == 0:
            logging.info (i)

    logging.info(f'{i} end')

    logging.info(df)

    data_cols = [x['name'] for x in df_abi[df_abi['name']=='getCreditAccountData']['abi'].values[0]['outputs'][0]['components']]

    batchtime = datetime.utcnow()
    df['batchtime'] = batchtime

    for ids in list(chunks(list(df[pd.notna(df['Borrower'])].loc[:,'id']), 1000)): #chunk size for multicall = 1000 (reduce in case of issues)
        id_range = list(df['id'].isin(ids))
        try:
            d_data = get_data_multicall(w3_eth, df.loc[id_range], 'getCreditAccountData', df_abi, DataCompressor)
        except ContractLogicError:
            logging.error('getCreditAccountData: Multicall error, calling it one by one')
            d_data = {x:getCreditAccountData(x,y,z) for x,y,z in zip(df.loc[id_range]['id'],df.loc[id_range]['CM'],df.loc[id_range]['Borrower'])}
            d_data = AttributeDict.recursive(d_data)
        for data in data_cols:
            if data not in ['balances', 'inUse', 'borrower', 'addr', 'borrower', 'creditManager', 'since']: #duplicate columns
                df.loc[id_range, data] = df.loc[id_range].apply(lambda x: {y:z for y, z in zip(data_cols, d_data[x['id']])}[data] if d_data[x['id']] else None
                                                                , axis=1)

        tokens_to_frame = [allowedTokens[x]['symbol'] for x in allowedTokens]
        for token in tokens_to_frame:
            df.loc[id_range, 'Balance_'+token] = df.loc[id_range].apply(lambda x: get_token_balance(x, token, data_cols, d_data, allowedTokens)
                                                            , axis=1)

    logging.info(df)
    logging.info(df[pd.notna(df['Borrower'])]) # active CAs
    logging.info(f'batchtime {batchtime}' )
    logging.info(f'countCreditAccounts - countCreditAccountsInStock = {countCreditAccounts - countCreditAccountsInStock}')

    #For compatability with BQ data types
    numeric_cols = [x for x in df.columns if x not in ['CA', 'CM', 'CF' ,'Symbol', 'Borrower', 'batchtime',
                                                          'underlyingToken', 'underlying', 'canBeClosed']]
    df[numeric_cols] = df[numeric_cols].astype('float64')
    df['canBeClosed'] =  df['canBeClosed'].astype('bool')
    df.columns = [x.replace('-','_') for x in df.columns]

    logging.info(df.dtypes)

    df_price_oracle = pd.DataFrame.from_dict([allowedTokens[x] for x in allowedTokens])
    df_price_oracle = df_price_oracle.set_index('symbol', drop=True).transpose()
    df_price_oracle.columns = ['Price_'+x for x in df_price_oracle.columns]
    df_price_oracle[[x.replace('Price','Decimals') for x in df_price_oracle.columns]] = df_price_oracle.loc['decimals',:]
    df_price_oracle = df_price_oracle.drop(index = 'decimals')

    df_price_oracle = df_price_oracle.reset_index()
    df_price_oracle = df_price_oracle.rename(columns={'index': 'Price_Asset'})
    df_price_oracle.columns = [x.replace('-','_') for x in df_price_oracle.columns]
    df_price_oracle['Price_Asset'] = df_price_oracle['Price_Asset'].apply(lambda x: x.replace('Price_',''))

    df_price_oracle['batchtime'] = batchtime

    #For compatability with BQ data types
    numeric_cols = [x for x in df_price_oracle.columns if x not in ['Price_Asset', 'batchtime']]
    df_price_oracle[numeric_cols] = df_price_oracle[numeric_cols].astype('float64')

    logging.info(df_price_oracle)

    df_token_price = pd.DataFrame.from_dict([allowedTokens[x] for x in allowedTokens]);
    df_token_price.columns = [x.replace('Price_','') for x in df_token_price.columns]
    df_token_price['batchtime'] = batchtime
    df_token_price = df_token_price.rename(columns = {'symbol':'token'})
    numeric_cols = [x for x in df_token_price.columns if x not in ['token', 'batchtime','decimals']]
    df_token_price[numeric_cols] = df_token_price[numeric_cols]/1e18
    df_token_price[numeric_cols] = df_token_price[numeric_cols].astype('float64')
    df_token_price.columns = [x.replace('-','_') for x in df_token_price.columns]

    logging.info(df_token_price)

    credentials = service_account.Credentials.from_service_account_file(
        'gearbox-336415-5ed144668529.json',
    )
    gcp_project_id = 'gearbox-336415'

    pandas_gbq.to_gbq(df,
                      'gearbox.credit_account',
                      project_id=gcp_project_id,
                      if_exists = 'replace',
                      progress_bar = False)
    logging.info('gearbox.credit_account, insert done')

    pandas_gbq.to_gbq(df_price_oracle,
                      'gearbox.price_oracle',
                      project_id=gcp_project_id,
                      if_exists = 'append',
                      progress_bar = False)
    logging.info('gearbox.price_oracle, insert done')

    pandas_gbq.to_gbq(df_token_price,
                      'gearbox.token_price',
                      project_id=gcp_project_id,
                      if_exists = 'append',
                      progress_bar = False)
    logging.info('gearbox.token_price, insert done')

    #Open, Close, Repay, Liquidate
    OpenCreditAccount_topic             =  df_abi[(df_abi['name']=='OpenCreditAccount') &(df_abi['contract_type']=='creditFacade') & (df_abi['type']=='event')]['topic'].values[0]
    OpenCreditAccount_topic_v1          =  df_abi[(df_abi['name']=='OpenCreditAccount') &(df_abi['contract_type']=='CreditManager_v1') & (df_abi['type']=='event')]['topic'].values[0]
    AddCollateral_topic                 =  df_abi[(df_abi['name']=='AddCollateral') & (df_abi['contract_type']=='creditFacade') & (df_abi['type']=='event')]['topic'].values[0]
    AddCollateral_topic_v1              =  df_abi[(df_abi['name']=='AddCollateral') & (df_abi['contract_type']=='CreditManager_v1') & (df_abi['type']=='event')]['topic'].values[0]
    CloseCreditAccount_topic            =  df_abi[(df_abi['name']=='CloseCreditAccount') & (df_abi['contract_type']=='creditFacade') & (df_abi['type']=='event')]['topic'].values[0]
    CloseCreditAccount_topic_v1         =  df_abi[(df_abi['name']=='CloseCreditAccount') & (df_abi['contract_type']=='CreditManager_v1') & (df_abi['type']=='event')]['topic'].values[0]
    RepayCreditAccount_topic_v1         =  df_abi[(df_abi['name']=='RepayCreditAccount') & (df_abi['contract_type']=='CreditManager_v1') & (df_abi['type']=='event')]['topic'].values[0]
    LiquidateCreditAccount_topic        =  df_abi[(df_abi['name']=='LiquidateCreditAccount') & (df_abi['contract_type']=='creditFacade') & (df_abi['type']=='event')]['topic'].values[0]
    LiquidateCreditAccount_topic_v1     =  df_abi[(df_abi['name']=='LiquidateCreditAccount') & (df_abi['contract_type']=='CreditManager_v1') & (df_abi['type']=='event')]['topic'].values[0]
    LiquidateExpiredCreditAccount_topic =  df_abi[(df_abi['name']=='LiquidateExpiredCreditAccount')  & (df_abi['contract_type']=='creditFacade') &(df_abi['type']=='event')]['topic'].values[0]

    try:
        df_blocknum = pandas_gbq.read_gbq('select max(blockNumber) from gearbox.account_events',
                                 project_id=gcp_project_id,
                                 progress_bar_type = None,)
        if pd.notna(df_blocknum.iloc[0,0]):
            from_block = int(df_blocknum.iloc[0,0]) + 1
        else:
            from_block = 0
    except pandas_gbq.exceptions.GenericGBQException:
        logging.error('The table does not exist?')
        from_block = 0

    logging.info(f'from_block={from_block}')

    df_events = pd.DataFrame()

    for CM in CreditManagers:
        logging.info(f'get_logs from CM {CM}')
        logs_v1 = get_logs(w3_eth, CM, df_abi,
                        [[OpenCreditAccount_topic_v1,
                          AddCollateral_topic_v1,
                          CloseCreditAccount_topic_v1,
                          RepayCreditAccount_topic_v1,
                          LiquidateCreditAccount_topic_v1,]
                        ],
                        from_block,
                        'latest')
        df_events = df_events.append(logs_v1, ignore_index = True)

        if cm_dict[CM]['creditFacade']:
            logging.info(f'get_logs from CF {cm_dict[CM]["creditFacade"]}')
            logs = get_logs(w3_eth, cm_dict[CM]['creditFacade'], df_abi,
                            [[OpenCreditAccount_topic,
                              AddCollateral_topic,
                              CloseCreditAccount_topic,
                              LiquidateCreditAccount_topic,
                              LiquidateExpiredCreditAccount_topic]
                            ],
                            from_block,
                            'latest')
            df_events = df_events.append(logs, ignore_index = True)

    logging.info(f'number of events={len(df_events)}')
    cf_dict = {cm_dict[y]['creditFacade']:{'CreditManager':y} for y in cm_dict if cm_dict[y]['creditFacade']}

    if len(df_events)>0:
        df_events['CF'] = df_events['address'].apply(lambda x: x if x in cf_dict else None)
        df_events['CM'] = df_events['address'].apply(lambda x: x if x in cm_dict else cf_dict[x]['CreditManager'])


        df_events['CM_Token'] = df_events['CM'].apply(lambda x: cm_dict[x]['symbol'])
        df_events['blockHash'] = df_events['blockHash'].apply(lambda x: x.hex())
        df_events['transactionHash'] = df_events['transactionHash'].apply(lambda x: x.hex())
        df_events['Borrower'] = df_events.apply(lambda x: x['args']['to']
                                                      if x['event'] in ['CloseCreditAccount','CloseCreditAccount']
                                                      else x['args']['onBehalfOf'] if x['event'] in ['OpenCreditAccount','AddCollateral']
                                                      else x['args']['borrower'] if x['event'] in ['IncreaseBorrowedAmount']
                                                      else x['args']['owner'] if x['event'] in ['LiquidateCreditAccount', 'LiquidateExpiredCreditAccount']
                                                      else None
                                            ,axis=1)
        df_events['Liquidator'] = df_events.apply(lambda x: x['args']['liquidator'] if x['event'] in ['LiquidateCreditAccount','LiquidateExpiredCreditAccount']
                                                      else None
                                            ,axis=1)
        df_events['Collateral_Token'] = df_events.apply(lambda x: allowedTokens[x['args']['token']]['symbol'] if x['event']=='AddCollateral'
                                                        else cm_dict[x['CM']]['symbol']
                                            ,axis=1)
        df_events['Collateral'] = df_events.apply(lambda x: x['args']['amount']*(10**-cm_dict[x['CM']]['decimals']) if x['event']=='OpenCreditAccount' and 'amount' in x['args']
                                                        else x['args']['value']*(10**-allowedTokens[x['args']['token']]['decimals']) if x['event']=='AddCollateral'
                                                        else None
                                            ,axis=1)

        logging.info('pull event timestamp begin')
        df_events['blockTimestamp'] = df_events['blockNumber'].apply(lambda x: datetime.utcfromtimestamp(w3_eth.eth.getBlock(x)['timestamp']))
        logging.info('pull event timestamp end')

        logging.info('pull transaction fee begin')
        df_events['transaction_receipt'] = df_events['transactionHash'].apply(lambda x: dict(w3_eth.eth.get_transaction_receipt(x)))
        df_events['transaction_fee'] = df_events['transaction_receipt'].apply(lambda x: x['gasUsed']*x['effectiveGasPrice']/1e18)
        logging.info('pull transaction fee end')

        df_events[['args','transaction_receipt']] = df_events[['args','transaction_receipt']].apply(str)
        df_events['Liquidator'] = df_events['Liquidator'].astype('object')
        df_events['Collateral'] = df_events['Collateral'].astype('float64')
        df_events.drop(columns = 'logIndex')
        df_events = df_events.sort_values('blockTimestamp', ignore_index = True)

        logging.info(df_events)

        pandas_gbq.to_gbq(df_events,
                          'gearbox.account_events',
                          project_id=gcp_project_id,
                          if_exists = ('replace' if from_block==0 else 'append'),
                          progress_bar = False)
        logging.info('gearbox.account_events, insert done')
    else:
        logging.info(f'No new events since block {from_block}')

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(funcName)s %(levelname)s %(message)s',
        level=os.environ.get('LOGLEVEL', 'INFO').upper(),
        datefmt='%Y-%m-%d %H:%M:a%S',
        )
    w3_eth = Web3(Web3.HTTPProvider(RPC_Endpoint, request_kwargs={'timeout': 30}))
    logging.info (f'Ethereum connected: {w3_eth.isConnected()}')

    main()
