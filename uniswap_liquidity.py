import os
import logging
import pandas as pd
from datetime import datetime
from web3 import Web3
from web3.exceptions import BadFunctionCallOutput, ContractLogicError
from multicall import Call, Multicall
from google.oauth2 import service_account
import pandas_gbq

abi="""[
     {"name":"factory",
      "inputs":[],"constant":true, "outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view",
      "type":"function"}
    ,{"name":"getPool",
      "inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"},{"internalType":"uint24","name":"","type":"uint24"}],"outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view",
      "type":"function"}
    ,{"name":"getPair",
      "inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","constant":true,
      "type":"function"}
    ,{"name":"balanceOf",
      "inputs":[{"name":"","type":"address"}],
      "outputs":[{"name":"","type":"uint256"}],"constant":true, "payable":false,"stateMutability":"view",
      "type":"function"}
    ,{"name":"decimals",
      "inputs":[],"outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view",
      "type":"function"}

    ]
    """
# The dafault RPC from ethersjs, change it if it doesn't work: https://infura.io/docs
RPC_Endpoint = 'https://mainnet.infura.io/v3/84842078b09946638c03157f83405213'

UniV2Router = '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D'
UniV3Router = '0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45'
UniV3Fee    = [100, 500, 3000, 10000]

token_base = {'WETH':'0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
              'WBTC':'0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599',
              'USDC':'0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
               'DAI':'0x6B175474E89094C44Da98b954EedeAC495271d0F'}

token_dict = {'WETH':'0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
              'WBTC':'0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599',
              'USDC':'0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
               'DAI':'0x6B175474E89094C44Da98b954EedeAC495271d0F',
             '1INCH':'0x111111111117dC0aa78b770fA6A738034120C302',
              'AAVE':'0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9',
              'COMP':'0xc00e94Cb662C3520282E6f5717214004A7f26888',
               'DPI':'0x1494CA1F11D487c2bBe4543E90080AeBa4BA3C2b',
               'FEI':'0x956F47F50A910163D8BF957Cf5846D573E7f87CA',
              'LINK':'0x514910771AF9Ca656af840dff83E8264EcF986CA',
               'SNX':'0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F',
               'UNI':'0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984',
               'YFI':'0x0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e',
              'LUNA':'0xd2877702675e6cEb975b4A1dFf9fb7BAF4C91ea9',
               'FTM':'0x4E15361FD6b4BB609Fa63C81A2be19d873717870',
             'SUSHI':'0x6B3595068778DD592e39A122f4f5a5cF09C90fE2',
               'CRV':'0xD533a949740bb3306d119CC777fa900bA034cd52',
               'CVX':'0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B',
               'LDO':'0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32',
             'STETH':'0xDFe66B14D37C77F4E9b180cEb433d1b164f0281D',
              'FRAX':'0x853d955aCEf822Db058eb8505911ED77F175b99e',
              'LUSD':'0x5f98805A4E8be255a32880FDeC7F6728C6568bA0',
              'SUSD':'0x57Ab1ec28D129707052df4dF418D58a2D46d5f51',
              'GUSD':'0x056Fd409E1d7A124BD7017459dFEa2F387b6d5Cd',
              'USDT':'0xdAC17F958D2ee523a2206206994597C13D831ec7',
              'FXS' :'0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0',
             'SPELL':'0x090185f2135308BaD17527004364eBcC2D37e5F6',
             'LQTY' :'0x6DEA81C8171D0bA574754EF6F8b412F2Ed88c54D',
            }
token_decimals = {}

def get_factory(w3, abi, Router):
    UniRouter = Web3.toChecksumAddress(Router)
    Factory = w3.eth.contract(address=UniRouter, abi=abi).functions.factory().call()

    return Factory

def get_token_decimals (w3, abi, token_address, symbol=None):
    token_address = Web3.toChecksumAddress(token_address)
    if token_address not in token_decimals:
        decimals = w3_eth.eth.contract(address=token_address, abi=abi).functions.decimals().call()
        token_decimals[token_address] = decimals
        if symbol:
            token_decimals[symbol] = decimals

    return token_decimals[token_address]


def main():
    pools = {}

    factory2 = get_factory(w3_eth, abi, UniV2Router)
    logging.info(f'factory2: {factory2}')
    factory3 = get_factory(w3_eth,abi, UniV3Router)
    logging.info(f'factory3: {factory3}')

    for symbol0 in token_base:
        token0 = Web3.toChecksumAddress(token_base[symbol0])
        base_decimals = get_token_decimals(w3_eth, abi, token0, symbol0)

        logging.info(f'pools count= {len(pools)}..')

        for symbol1 in token_dict:
            token1 = Web3.toChecksumAddress(token_dict[symbol1])
            token_decimals = get_token_decimals(w3_eth, abi, token1, symbol1)

            pool = w3_eth.eth.contract(address=factory2, abi=abi).functions.getPair(token0,token1).call()
            pools[pool] = {'token'         :symbol1,
                           'token_decimals':token_decimals,
                           'base_token'    :symbol0,
                           'base_decimals' :base_decimals,
                           'version'       :'Uniswap V2',
                           'fee' : None,}

            multi_getPool = Multicall([
                                Call(factory3, ['getPool(address,address,uint24)(address)', token0,token1,x], [[x, Web3.toChecksumAddress]]) for x in UniV3Fee
                                ]
                                ,_w3 = w3_eth)

            multi_getPool = multi_getPool()

            pools.update({ multi_getPool[fee]:{'token':symbol1,
                                               'token_decimals':token_decimals,
                                               'base_token':symbol0,
                                               'base_decimals':base_decimals,
                                               'version':'Uniswap V3',
                                               'fee' : fee/10**6}
                          for fee in multi_getPool})

    logging.info(f'pools count= {len(pools)}')

    for pool in list(pools.keys()):
        if pool=='0x0000000000000000000000000000000000000000':
            pools.pop(pool)

    multi_balanceOwn = Multicall([Call(token_dict[pools[x]['token']],
                                      ['balanceOf(address)(uint256)', x],
                                      [[x, None]]
                                     ) for x in pools
                                ]
                                ,_w3 = w3_eth)

    multi_balanceOwn = multi_balanceOwn()


    multi_balanceBase = Multicall([Call(token_base[pools[x]['base_token']],
                                      ['balanceOf(address)(uint256)', x],
                                      [[x, None]]
                                     ) for x in pools
                                ]
                                ,_w3 = w3_eth)

    multi_balanceBase = multi_balanceBase()

    {pools[x].update({  'OwnBalance'           : multi_balanceOwn[x]*10**(-pools[x]['token_decimals']),
                        pools[x]['base_token'] : multi_balanceBase[x]*10**(-pools[x]['base_decimals']),
                     })
         for x in multi_balanceBase}

    logging.info(f'pools final count= {len(pools)}')

    return pools

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(funcName)s %(levelname)s %(message)s',
        level=os.environ.get('LOGLEVEL', 'INFO').upper(),
        datefmt='%Y-%m-%d %H:%M:a%S',
        )
    w3_eth = Web3(Web3.HTTPProvider(RPC_Endpoint, request_kwargs={'timeout': 20}))
    logging.info (f'Ethereum connected: {w3_eth.isConnected()}')

    ret = main()

    df = pd.DataFrame.from_dict(ret, orient = 'index').reset_index().rename(columns = {'index':'pool'})
    df['batchtime'] = datetime.utcnow()
    logging.info(df)

    credentials = service_account.Credentials.from_service_account_file(
    'gearbox-336415-5ed144668529.json',
    )
    gcp_project_id = 'gearbox-336415'

    pandas_gbq.to_gbq(df,
                      'gearbox.dex_liquidity',
                      project_id=gcp_project_id,
                      if_exists = 'append',
                      progress_bar = False)
    logging.info('gearbox.dex_liquidity, insert done')
