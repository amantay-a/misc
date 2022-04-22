import logging
from web3 import Web3
from web3.exceptions import BadFunctionCallOutput

abi="""[
     {"name":"factory",
      "inputs":[],"constant":true, "outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view",
      "type":"function"}
    ,{"name":"decimals",
      "inputs":[],"outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view",
      "type":"function"}
    ,{"name":"getPool",
      "inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"},{"internalType":"uint24","name":"","type":"uint24"}],"outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view",
      "type":"function"}
    ,{"name":"observe",
      "inputs":[{"internalType":"uint32[]","name":"secondsAgos","type":"uint32[]"}],"outputs":[{"internalType":"int56[]","name":"tickCumulatives","type":"int56[]"},{"internalType":"uint160[]","name":"secondsPerLiquidityCumulativeX128s","type":"uint160[]"}],"stateMutability":"view",
      "type":"function"}
    ,{"name":"token0",
      "inputs":[],"outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view",
      "type":"function"}
    ,{"name":"token1",
      "inputs":[],"outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view",
      "type":"function"}
    ]
    """

UniV3Router = '0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45'
UniPoolFee = 3000 #0.3%

decimals = {}
pools = {}
tokens0 = {}
accFactory = {}

def getPoolPrice(w3,
                 tokensA,
                 tokensB = {'0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'}, #Default WETH
                 TWAPWindows = [60, 300, 600, 3600], #measured in seconds
                 blocks = ['latest'], #latest by default
                 PoolFee = UniPoolFee, #3000 for 0.3%
                 Router = UniV3Router,
                ):
    ret_price = {}
    UniRouter = Web3.toChecksumAddress(Router)
    if not UniRouter in accFactory:
        accFactory[UniRouter] = w3.eth.contract(address=UniRouter, abi=abi).functions.factory().call()
    UniFactory = accFactory[UniRouter]

    logging.info(f'UniV3Factory: {UniFactory}')
    if PoolFee <=1:
        PoolFee*=10**6
    for t_a in tokensA:
        t_a = Web3.toChecksumAddress(t_a)
        if not t_a in decimals:
            decimals[t_a] = w3.eth.contract(address=t_a, abi=abi).functions.decimals().call()
        decimals_a = decimals[t_a]

        for t_b in tokensB:
            t_b = Web3.toChecksumAddress(t_b)
            if not t_b in decimals:
                decimals[t_b] = w3.eth.contract(address=t_b, abi=abi).functions.decimals().call()
            decimals_b = decimals[t_b]

            pool = [x for x in pools if pools[x] == {t_a, t_b, PoolFee}]
            if not pool:
                pool = w3.eth.contract(address=UniFactory, abi=abi).functions.getPool(t_a, t_b, PoolFee).call()
                t0 = w3.eth.contract(address=pool, abi=abi).functions.token0().call()
                pools[pool] = {t_a, t_b, PoolFee}
                tokens0[pool] = t0
            else:
                pool = pool[0]
                t0 = tokens0[pool]

            if t0==t_a:
                t1 = t_b
                decimals0 = decimals_a
                decimals1 = decimals_b
            else:
                t1=t_a
                decimals0 = decimals_b
                decimals1 = decimals_a

            logging.info(f'pool: {pool}, token0={t0}, decimals0={decimals0}, token1={t1}, decimals1={decimals0}')

            for blockNumber in blocks:
                try:
                    ticks = w3.eth.contract(address=pool, abi=abi).functions.observe(TWAPWindows+[0]).call(block_identifier = blockNumber)[0]

                    price = [1.0001**((ticks[-1] - ticks[i])/x) for i, x in enumerate(TWAPWindows)]
                    logging.debug(f'block={blockNumber}, pool: {pool}, tick price= {price}')

                    price = [x*pow(10, decimals0)/pow(10, decimals1) for x in price]
                    logging.debug(f'block={blockNumber}, pool: {pool}, decimal price= {price}')

                    if t0!=t_a: #reverse price
                        price = [1/x for x in price]
                        logging.debug(f'block={blockNumber}, pool: {pool}, reverse price= {price}')

                    price = {TWAPWindows[i]:x for i, x in enumerate(price)}

                except BadFunctionCallOutput as err:
                    price = {TWAPWindows[i]:None for i, x in enumerate(TWAPWindows)}
                    logging.error(f'block={blockNumber}, pool: {pool}, error: {err}')

                logging.info(f'block={blockNumber}, pool: {pool}, step, price= {price}')
                if blockNumber in ret_price:
                    ret_price[blockNumber][t_a+'-'+t_b] = {'pool':pool, 'twap': price}
                else:
                    ret_price[blockNumber] = {t_a+'-'+t_b :{'pool':pool, 'twap': price}}

    return ret_price
