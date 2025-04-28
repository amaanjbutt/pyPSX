from datetime import datetime, timedelta
from typing import Optional, Union, List, Dict, Any
import pandas as pd

from .fetchers import fetch_intraday_data, fetch_historical_data, scrape_historical_data
from .exceptions import PSXRequestError
from .tradingview import TradingViewClient
from .psx_reader import PSXDataReader, psx_reader

__all__ = ['PSXTicker', 'PSXDataReader']

class PSXTicker:
    def __init__(self, symbol: str):
        """
        Initialize a PSX ticker instance
        
        Args:
            symbol: Stock symbol (e.g. 'HBL')
        """
        self.symbol = symbol.upper()
        
    def get_intraday_data(self) -> pd.DataFrame:
        """
        Get current intraday data
        
        Returns:
            DataFrame containing timestamp-indexed price and volume data
        """
        try:
            return psx_reader.get_intraday_data(self.symbol)
        except Exception as e:
            # Fallback to old method if new method fails
            try:
                data = fetch_intraday_data(self.symbol)
                return pd.DataFrame([{
                    'timestamp': datetime.now(),
                    'price': data.get('price', None),
                    'volume': data.get('volume', None)
                }]).set_index('timestamp')
            except Exception as e2:
                raise PSXRequestError(f"Failed to fetch intraday data: {str(e2)}")
        
    def get_historical_data(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        period: str = "1mo",
        use_tradingview: bool = True,
        use_psx_reader: bool = True
    ) -> pd.DataFrame:
        """
        Get historical daily data
        
        Args:
            start_date: Start date for data (default: period ago)
            end_date: End date for data (default: today)
            period: Time period (e.g. '1mo', '3mo', '6mo', '1y')
            use_tradingview: Whether to try TradingView as a data source
            use_psx_reader: Whether to try PSXDataReader as a data source
            
        Returns:
            DataFrame with historical OHLCV data
        """
        if end_date is None:
            end_date = datetime.now()
            
        if start_date is None:
            # Parse period string
            value = int(period[:-2])
            unit = period[-2:]
            
            if unit == 'mo':
                start_date = end_date - timedelta(days=value * 30)
            elif unit == 'y':
                start_date = end_date - timedelta(days=value * 365)
            else:
                raise ValueError("Invalid period format. Use 'Xmo' or 'Xy' (e.g. '1mo', '1y')")
        
        # Try different data sources in order of preference
        errors = []
        
        # 1. Try PSXDataReader first (most reliable)
        if use_psx_reader:
            try:
                df = scrape_historical_data(self.symbol, start_date, end_date)
                
                # Ensure column names are lowercase
                df.columns = [col.lower() for col in df.columns]
                
                # Calculate statistics
                if not df.empty:
                    print(f"\nTotal days of data: {len(df)}")
                    print("\nFirst 5 entries:")
                    print(df.head())
                    print("\nLast 5 entries:")
                    print(df.tail())
                    print("\nPrice Statistics:")
                    print(f"Average price: {df['close'].mean():.2f}")
                    print(f"Highest price: {df['high'].max():.2f}")
                    print(f"Lowest price: {df['low'].min():.2f}")
                    print(f"Total volume: {df['volume'].sum():,.0f}")
                
                return df
            except PSXRequestError as e:
                errors.append(f"PSXDataReader error: {str(e)}")
        
        # 2. Try TradingView if enabled
        if use_tradingview:
            try:
                tv_client = TradingViewClient()
                tv_data = tv_client.get_historical_data(
                    symbol=self.symbol,
                    start_date=start_date,
                    end_date=end_date
                )
                
                # Convert TradingView data to DataFrame if needed
                if isinstance(tv_data, dict) and 'data' in tv_data:
                    # Extract data from TradingView response
                    data = []
                    for entry in tv_data['data']:
                        if isinstance(entry, list) and len(entry) >= 6:
                            date_str, open_val, high, low, close, volume = entry
                            data.append({
                                'date': pd.to_datetime(date_str),
                                'open': open_val,
                                'high': high,
                                'low': low,
                                'close': close,
                                'volume': volume
                            })
                    
                    df = pd.DataFrame(data)
                    if not df.empty:
                        df.set_index('date', inplace=True)
                        return df
            except Exception as e:
                errors.append(f"TradingView error: {str(e)}")
        
        # If all methods fail, raise an error with details
        error_msg = "Failed to fetch historical data using all available methods:\n"
        for i, error in enumerate(errors, 1):
            error_msg += f"{i}. {error}\n"
        
        raise PSXRequestError(error_msg) 