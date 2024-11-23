import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import itertools
import os

class MT5Extractor:
    TIMEFRAME_MAP = {
    mt5.TIMEFRAME_M1: 'M1',
    mt5.TIMEFRAME_M5: 'M5',
    mt5.TIMEFRAME_M15: 'M15',
    mt5.TIMEFRAME_M30: 'M30',
    mt5.TIMEFRAME_H1: 'H1',
    mt5.TIMEFRAME_H4: 'H4',
    mt5.TIMEFRAME_D1: 'D1',
    mt5.TIMEFRAME_W1: 'W1',
    mt5.TIMEFRAME_MN1: 'MN1'}


    def __init__(self):
        if not mt5.initialize():
            raise RuntimeError("Error initializing MetaTrader5")
        self.data = None
    
    def extract_data(self, symbol, timeframe):
        end_date = datetime.now()
        start_date = end_date - relativedelta(years=10)
        symbol_info = mt5.copy_rates_range(symbol,timeframe,start_date,end_date)
        symbol_info_df = pd.DataFrame(symbol_info)
        symbol_info_df['time'] = pd.to_datetime(symbol_info_df['time'], unit='s')
        if len(symbol_info_df) == 0:
            raise ValueError(f'No data was found for {symbol} with timeframe {timeframe}. Start Date: {start_date} and End Date: {end_date}.')
        return symbol_info_df
    
    def extract_multiple_data(self, symbols, timeframes):
        symbols_all_tfs = itertools.product(symbols,timeframes)
        all_info = []
        for s_and_tf in symbols_all_tfs:
            symbol_info_df = self.extract_data(s_and_tf[0],s_and_tf[1])
            symbol_info_df['ticket'] = s_and_tf[0]
            symbol_info_df['timeframe'] = self.TIMEFRAME_MAP.get(s_and_tf[1], f"Unknown ({s_and_tf[1]})")
            all_info.append(symbol_info_df)
        self.data = pd.concat(all_info)

    def save_to_parquet(self, filepath, partition_cols=None):
        if self.data is None:
            raise ValueError("No data available to save. Please run `extract_multiple_data` first.")

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        if partition_cols:
            self.data.to_parquet(filepath, index=False, partition_cols=partition_cols, engine='pyarrow')
        else:
            self.data.to_parquet(filepath, index=False, engine='pyarrow')
        print(f"Arquivo salvo em: {filepath}")

    def close(self):
        """
        Finaliza a conexão com o MetaTrader5.
        """
        mt5.shutdown()

    def close(self):
        mt5.shutdown()    

####### USAGE EXAMPLE #######
# tickers = ['WDO$','WDO$D','WDO$N','WIN$','WIN$D','WIN$N']
# timeframes = [mt5.TIMEFRAME_M1,mt5.TIMEFRAME_M5,mt5.TIMEFRAME_M15,mt5.TIMEFRAME_M30,mt5.TIMEFRAME_H1,mt5.TIMEFRAME_D1]
# mt = MT5Extractor()
# mt.extract_multiple_data(tickers,timeframes)
# mt.save_to_parquet('data/mini_futuros_2024_11_23.parquet')