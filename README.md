# air888
air888
with 
uniswap AS (
    SELECT DISTINCT account, 'Uniswap' as airdrops
    FROM uniswap."MerkleDistributor_evt_Claimed" 
    WHERE contract_address='\x090d4613473dee047c3f2706764f49e0821d256e'
),
oneinch AS (
    SELECT DISTINCT "to" AS account, '1inch' as airdrops
    FROM oneinch."OneInch_evt_Transfer" 
    WHERE "from"='\xe295ad71242373c37c5fda7b57f26f9ea1088afe' /*AND value != 0*/
),
gitcoin AS (
    SELECT DISTINCT "to" AS account, 'Gitcoin' as airdrops
    FROM gitcoin."GTC_evt_Transfer" 
    WHERE "from"='\xde3e5a990bce7fc60a6f017e7c4a95fc4939299e' /*AND amount != 0*/
),
dydx AS (
    SELECT DISTINCT "to" AS account, 'Dydx' as airdrops
    FROM erc20."ERC20_evt_Transfer"
    WHERE contract_address='\x92d6c1e31e14520e676a687f0a93788b716beff5' AND "from"='\x639192D54431F8c816368D3FB4107Bc168d0E871' /*AND value != 0*/
),
ens AS (
    SELECT DISTINCT "claimant" AS account, 'ENS' as airdrops
    FROM ethereumnameservice."ENSToken_evt_Claim"
),
paraswap AS (
    SELECT DISTINCT "to" AS account, 'Paraswap' as airdrops
    FROM erc20."ERC20_evt_Transfer"
    WHERE contract_address='\xcAfE001067cDEF266AfB7Eb5A286dCFD277f3dE5' AND "from"='\x090E53c44E8a9b6B1bcA800e881455b921AEC420' /*AND value != 0*/
),
ampleforth AS (
    SELECT DISTINCT "to" AS account, 'Ampleforth' as airdrops
    FROM erc20."ERC20_evt_Transfer" 
    WHERE contract_address='\x77fba179c79de5b7653f68b5039af940ada60ce0' /*AND value != 0*/
),
looksrare AS (
    SELECT DISTINCT "to" AS account, 'Looksrare' as airdrops
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from"='\xA35dce3e0E6ceb67a30b8D7f4aEe721C949B5970'
),
gold AS (
    SELECT DISTINCT "to" AS account, 'GoldFinch' as airdrops
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from" IN (
        '\x0Cd73c18C085dEB287257ED2307eC713e9Af3460',
        '\x7766e86584069Cf5d1223323d89486e95d9a8C22',
        '\x0f306E3f6b2d5ae820d33C284659B29847972d9A',
        '\xFD6FF39DA508d281C2d255e9bBBfAb34B6be60c3'
    ) /*AND value!=0*/
),
hop AS (
    SELECT DISTINCT "address" AS account, 'HOP' as airdrops
    FROM hop_protocol."airdrop_address_list"
),
graph AS (
    SELECT DISTINCT UNNEST("recipients") as account, 'TheGraph' as airdrops
    FROM disperse."Disperse_call_disperseToken"
    WHERE "call_tx_hash" IN (
        SELECT "tx_hash"
        FROM ethereum."traces"
        WHERE "from"='\x06590a641dc3eb43f2cebe435576389f209116da' AND "to"='\xd152f549545093347a162dce210e7293f1452150'
    )
),
torn AS (
    SELECT DISTINCT "to" AS account, 'TornadoCash' as airdrops
    FROM erc20."ERC20_evt_Transfer"
    WHERE contract_address='\x3efa30704d2b8bbac821307230376556cf8cc39e' /*AND value!=0*/
),
cow AS (
    with trader_volume AS (
        select trader          as owner,
               min(block_time) as first_trade,
               max(block_time) as last_trade,
               sum(case
                       when buy_token_address in
                            (select contract_address from erc20.stablecoins)
                           and
                            sell_token_address in
                            (select contract_address from erc20.stablecoins)
                           then 0
                       else trade_value_usd
                   end)        as non_stable_volume,
               sum(case
                       when buy_token_address in
                            (select contract_address from erc20.stablecoins)
                           and
                            sell_token_address in
                            (select contract_address from erc20.stablecoins)
                           then trade_value_usd
                       else 0
                   end)        as stable_volume,
               count(*)        as num_trades
        from gnosis_protocol_v2."trades"
                 join gnosis_protocol_v2."GPv2Settlement_evt_Settlement"
                      on tx_hash = evt_tx_hash
        group by owner
    )
    select DISTINCT t.owner as account, 'COW' as airdrops
    from trader_volume t
    where non_stable_volume + stable_volume > 0
),
across AS (
    SELECT DISTINCT "to" AS account, 'Across' as airdrops
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from"='\xE50b2cEAC4f60E840Ae513924033E753e2366487' /*AND value!=0*/
),
bico AS (
    SELECT DISTINCT "to" AS account, 'BICO' as airdrops
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from"='\xf92cdb7669a4601dd76b728e187f2a98092b6b7d' /*AND value!=0*/
),
hashflow AS (
    SELECT DISTINCT "to" AS account, 'Hashflow' as airdrops
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from"='\x1a9a4d919943340b7e855e310489e16155f4ed29' /*AND value!=0*/
),
x2y2 AS (
    SELECT DISTINCT "to" AS account, 'x2y2' as airdrops
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from"='\xe6949137b24ad50cce2cf6b124b3b874449a41fa' /*AND value!=0*/
),
galaxy AS (
    SELECT DISTINCT "to" AS account, 'Galaxy' as airdrops
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from"='\x92e130d5ed0f14199edfa050071116ca60e99aa5' /*AND value!=0*/
),
ribbon AS (
    SELECT DISTINCT "to" as account, 'Ribbon' as airdrops
    FROM ethereum."logs" log JOIN erc20."ERC20_evt_Transfer" t ON t.evt_tx_hash = log.tx_hash
    WHERE log.contract_address='\x7902e4bfb1eb9f4559d55417aee1dc6e4b8cc1bf'
),
inverse AS (
    SELECT DISTINCT "to" as account, 'Inverse' as airdrops
    FROM erc20."ERC20_evt_Transfer"
    WHERE "contract_address"='\x41D5D79431A913C4aE7d69a668ecdfE5fF9DFB68' AND "from"='\xe810281d189f19572b5250556369c39f5ebc6b00'
), 
dapp AS (
    SELECT "from" as account, 'DappRadar' as airdrops
    FROM ethereum."traces"
    WHERE "to"='\x2e424a4953940ae99f153a50d0139e7cd108c071'
),

table1 AS (
    SELECT round(((length(airdrop) - length(replace(airdrop, ',', ''))) / length(',')) + 1) AS counts, airdrop, account
    FROM (
        SELECT string_agg(airdrops, ',') AS airdrop, account
        FROM (
            SELECT account, airdrops 
            FROM uniswap
            UNION
            SELECT account, airdrops
            FROM oneinch
            UNION
            SELECT account, airdrops
            FROM gitcoin
            UNION
            SELECT account, airdrops
            FROM dydx
            UNION
            SELECT account, airdrops
            FROM ens
            UNION
            SELECT account, airdrops
            FROM paraswap
            UNION
            SELECT account, airdrops
            FROM ampleforth
            UNION
            SELECT account, airdrops
            FROM looksrare
            UNION
            SELECT account, airdrops
            FROM gold
            UNION
            SELECT account, airdrops
            FROM hop
            UNION
            SELECT account, airdrops
            FROM graph
            UNION
            SELECT account, airdrops
            FROM torn
            UNION
            SELECT account, airdrops
            FROM cow
            UNION
            SELECT account, airdrops
            FROM across
            UNION
            SELECT account, airdrops
            FROM bico
            UNION
            SELECT account, airdrops
            FROM hashflow
            UNION
            SELECT account, airdrops
            FROM x2y2
            UNION
            SELECT account, airdrops
            FROM galaxy
            UNION
            SELECT account, airdrops
            FROM ribbon
            UNION
            SELECT account, airdrops
            FROM inverse
            UNION
            SELECT account, airdrops
            FROM dapp
        ) s
        GROUP BY 2
    ) t ORDER BY counts DESC
)

SELECT counts, airdrop, concat('0x', substring(encode(account, 'hex') from 1 for 40)) AS account 
FROM table1
WHERE counts > 2
