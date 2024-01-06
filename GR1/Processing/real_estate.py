import pandas as pd
import os

folder_path = '/home/leminhphuc/real_estate_data/'

csv_files = ['bds123.csv', 'cenhomes.csv']

dfs = []
for csv_file in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    df = pd.read_csv(file_path)
    dfs.append(df)

result_df = pd.concat(dfs, ignore_index=True)

output_file = '/home/leminhphuc/real_estate_data/real_estate.csv'
result_df.to_csv(output_file, index=False)
