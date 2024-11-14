from spaceandtime import SpaceAndTime, SXTTable
from datetime import datetime, timedelta
import yfinance, pandas, os, re

 
# connect to the network, and authenticate (must have an .env file)
sxt = SpaceAndTime(envfile_filepath='./.env/secrets.env')
sxt.authenticate()

# create table object (appending your userid to the end of tablename):
stocks = SXTTable(name = f"SXTDemo.Stocks_{re.sub(r'[^a-zA-Z0-9_]', '_', sxt.user.user_id)}",
                  private_key = os.getenv('RESOURCE_PRIVATE_KEY'), 
                  access_type = sxt.TABLE_ACCESS.PUBLIC_READ, 
                  SpaceAndTime_parent=sxt)

stocks.create_ddl = """
    CREATE TABLE {table_name} 
    ( Symbol         VARCHAR
    ,Stock_Date     DATE 
    ,Stock_Open     DECIMAL
    ,Stock_High     DECIMAL
    ,Stock_Low      DECIMAL
    ,Stock_Close    DECIMAL
    ,Stock_AdjClose DECIMAL
    ,Stock_Volume   BigInt
    ,PRIMARY KEY (Symbol,Stock_Date)
    ) {with}
"""

# create three permissions (biscuits), one Admin, one for Reading, and one just to Load
stocks.add_biscuit('Admin', sxt.GRANT.ALL)
stocks.add_biscuit('Load', sxt.GRANT.INSERT, sxt.GRANT.UPDATE,
                           sxt.GRANT.DELETE, sxt.GRANT.SELECT)
stocks.add_biscuit('Read', sxt.GRANT.SELECT)

# save all table settings to file
stocks.save(stocks.recommended_filename)

# actually create, if missing:
if not stocks.exists: stocks.create()

# ----------------------------------------
# ------- PULL AND LOAD DATA -------------
# ----------------------------------------
# define start/end dates (7 rolling days)
end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

for symbol in ['AAPL', 'GOOGL', 'AMZN', 'MSFT', 'RIVN', 'NVDA',
               'BTC-USD','ETH-USD','SOL-USD','ADA-USD','XRP-USD',
               'DOT-USD','LUNA-USD','DOGE-USD','AVAX-USD',
               'SHIB-USD','ALGO-USD','LTC-USD']:
    sxt.logger.info(f'Processing: {symbol} between {start_date} and {end_date}')

    # download price data
    data = yfinance.download(symbol, start=start_date, end=end_date)

    if data.empty or data is None:
        print(f"No new data detected for {symbol}")
    else: 
        
        # transform pandas dataframe to list of dicts
        data = pandas.DataFrame(data).reset_index() # add date (from index)
        data.insert(loc=0, column='Symbol', value=symbol) # add symbol to front
        data.columns = ['Symbol', 'Stock_Date', 'Stock_Open', 'Stock_High', 'Stock_Low', 
                        'Stock_Close', 'Stock_AdjClose', 'Stock_Volume'] # rename to table column names
        data['Stock_Date'] = data['Stock_Date'].dt.strftime('%Y-%m-%d') # convert date to string
        data_load = data.to_dict(orient='records') # convert to list of dicts

        # delete any pre-existing data, before inserting:
        stocks.delete(where = f""" 
                    Symbol = '{symbol}' 
                    AND Stock_Date between '{start_date}' AND '{end_date}' """)

        # insert into SXT
        # success, response = stocks.insert.with_list_of_dicts(data_load)
        insert_head = f'INSERT INTO {stocks.table_name} ( {", ".join(list(data.columns))} ) VALUES \n ' 
        insert_body = '\n,'.join(['('+', '.join([f"'{c}'" for c in list(r.values())])+')' for r in data_load])
        success, response = stocks.insert.with_sqltext(insert_head + insert_body)
        if success: 
            sxt.logger.info('SUCCESSFUL INSERT!')
        else:
            sxt.logger.error(f'ERROR ON INSERT: {response}')

    

# Find the overall highest and lowest price for the time period
success, data = stocks.select(f"""
    select distinct Symbol
    , min( stock_low  ) over(partition by Symbol) as period_lowest_price
    , max( stock_high ) over(partition by Symbol) as period_highest_price
    from {stocks.table_name} 
    where Stock_Date between '{start_date}' and '{end_date}'
    order by Symbol  """)

if success: print('\n'.join([str(r) for r in data]))
print('\n\nDone!\n\n')