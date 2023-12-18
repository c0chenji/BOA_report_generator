import logging
import os
import pandas as pd
import glob

class FileProcessor:
  MARK_COLUMN_NAMES = ["Type", "MarketValue"]
  TRADING_COLUMN_NAMES = ['Date', 'Company', 'Type', 'Action', 'Quantity', 'Price']

  def __init__(self, folder_path = None) -> None:
    self.folder_path = folder_path

  def get_trading_data_file_list(self):
    file_path = os.path.join(self.folder_path, "xa*")
    trading_data_file_lst = glob.glob(file_path)
    return trading_data_file_lst

  def get_mark_data_df(self):
      try:
        mark_df = pd.read_csv(self.get_mark_data_file(),sep=' ', header=None, names= self.MARK_COLUMN_NAMES )
        return mark_df
      except FileNotFoundError as e:
        logging.error(f"Error: {e}")
        return None
      
  def get_trading_data_df(self):
      trading_df = []
      for file_path in self.get_trading_data_file_list():
          temp_data_df = pd.read_csv(file_path,sep='\t', header=None, names=self.TRADING_COLUMN_NAMES)
          trading_df.append(temp_data_df)
          
      combined_df = pd.concat(trading_df, ignore_index=True)
      return combined_df
          

  def get_mark_data_file(self):
      file_path = os.path.join(self.folder_path, "marks.txt")
      return file_path
 
  def process_files(self):
      mark_df = self.get_mark_data_df()
      trading_df = self.get_trading_data_df()
      if mark_df is not None and not trading_df.empty:
            final_df = pd.merge(trading_df, mark_df, on="Type", how="inner")
            return final_df
      else:
            return None 
          
  def company_top_values(self, value_type, top_num):
      df = self.process_files()
      if df is not None:
        df.loc[:,'Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
        df.loc[:,'Price'] = pd.to_numeric(df['Price'], errors='coerce')
        df.loc[:,'MarketValue'] = pd.to_numeric(df['MarketValue'], errors='coerce')
        if value_type == "asset":
          df.loc[:,"Total_Asset_Value"] = df['Quantity'] *df['Price']
          company_total_asset = df.groupby('Company')['Total_Asset_Value'].sum().reset_index()
          return company_total_asset.sort_values(by='Total_Asset_Value', ascending=False).head(top_num)
        
        elif value_type == "volume":
          company_total_volume = df.groupby('Company')['Quantity'].sum().reset_index()
          return company_total_volume.sort_values(by="Quantity",ascending=False).head(top_num)
        
        elif value_type == "market":
          df["Total_Market_Value"] = df['Quantity'] *df['MarketValue']
          company_market_value = df.groupby('Company')['Total_Market_Value'].sum().reset_index()
          return company_market_value.sort_values(by='Total_Market_Value', ascending=False).head(top_num) 
        
        else:
          raise ValueError("The value_type is not available.")     
      
  def company_actions(self,top_num):
      # Return action results sorted by long count where long is greater than short.
      df = self.process_files()
      df['Position'] = df['Action'].apply(lambda x: 1 if x == 'BUY' else -1)
      # Create separate DataFrames for long and short positions
      long_df = df[df['Position'] == 1]
      short_df = df[df['Position'] == -1]
      long_count = long_df.groupby('Company').size().reset_index(name='LongCount')
      short_count = short_df.groupby('Company').size().reset_index(name='ShortCount')
      df = pd.merge(df, long_count, on='Company', how='left')
      df = pd.merge(df, short_count, on='Company', how='left')
      
      # Fill NaN values with 0
      df['LongCount'] = df['LongCount'].fillna(0)
      df['ShortCount'] = df['ShortCount'].fillna(0)
      filtered_df =df[df['ShortCount'] < df['LongCount']]
      unique_companis = filtered_df[['Company', 'LongCount', 'ShortCount']].drop_duplicates()
      top_sorted_result = unique_companis.sort_values(by='LongCount', ascending=False).head(top_num)
      return top_sorted_result
      
      
if __name__ == '__main__':
    current_path = os.getcwd()
    folder_path = os.path.join(current_path, "test_data")
    temp_processor = FileProcessor(folder_path)
    
    print(temp_processor.company_actions(10))
    print(temp_processor.company_top_values("market",10))
    print(temp_processor.company_top_values("volume",10))
    print(temp_processor.company_top_values("asset",10))
    # print(temp_processor.company_total_volume(5))
    # print(temp_processor.company_total_asset(5))
    # print(temp_processor.company_total_market_value(5))
