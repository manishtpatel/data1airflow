import pandas as pd

rd = pd.read_csv('http://localhost:3000/public/train.csv')
print(rd.head())
print(rd['SaleCondition'].unique().tolist())
