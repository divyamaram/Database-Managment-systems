import pyodbc
import pandas as pd

# Connection steps to the server
from OnlineBankingPortalCSV2_code import Accounts, Customer

server = 'LAPTOP-SELQSNPH'
database = 'sai'
username = 'maram'
password = 'dima2k21'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# import data from csv
data = pd.read_csv (r'C:\Users\maram\PycharmProjects\pythonProject\OnlineBankingPortal_data_file3.csv')

# Transactions table
Transactions = pd.DataFrame(data, columns = ['Transaction_id','Acc_number','Transaction_type_code','Transaction_type_desc','Transaction_date','Card_number'])
Transactions = Transactions.astype('str')
Transactions['Transaction_id']=Transactions.groupby(['Transaction_date','Card_number'],sort=False).ngroup()+300

# Merge data inorder to get the required Id's
Merge_Transactions_Accounts=pd.merge(Transactions,Accounts,on='Acc_number')
Transactions['Account_id']=Merge_Transactions_Accounts.Account_id

Transactions['Customer_id']=Merge_Transactions_Accounts.Customer_id
print(Transactions)

Transactions['Transaction_date'] = Transactions['Transaction_date'].astype('datetime64[ns]')

# Cards table
Cards = pd.DataFrame(data, columns = ['Acc_number','Card_id','Card_number','Maximum_limit','Expiry_Date','Credit_score'])
Cards = Cards.astype('str')
Cards['Expiry_Date']= Cards['Expiry_Date'].astype('datetime64[ns]')

# Merge data inorder to get the required Id's
Merge_Cards_Accounts=pd.merge(Cards,Accounts,on='Acc_number')
Cards['Customer_id']=Merge_Cards_Accounts.Customer_id
Cards = Cards[Cards.Card_number != 'nan']
Cards['Card_id'] = Cards.groupby(['Card_number'],sort=False).ngroup()+400
Cards = Cards.drop_duplicates(subset=None, keep="first", inplace=False)

# Convert Credit score and Maximum limit from string->float->int
Cards['Credit_score']=Cards['Credit_score'].astype(float)
Cards['Credit_score']=Cards['Credit_score'].astype(int)
Cards['Maximum_limit']=Cards['Maximum_limit'].astype(float)
Cards['Maximum_limit']=Cards['Maximum_limit'].astype(int)
print(Cards)

# Transaction_details Table

Transaction_details = pd.DataFrame(data, columns = ['Transaction_Amount','Merchant_details','Acc_number','Transaction_date'])
Transaction_details = Transaction_details.astype('str')

# Merge data inorder to get the required Id's
Merge_Transaction_details_Transactions=pd.concat([Transactions,Transaction_details], ignore_index=True)
Transaction_details['Transaction_id']=Merge_Transaction_details_Transactions.Transaction_id

# Convert Transaction_id from string->float->int
Transaction_details['Transaction_id']=Transaction_details['Transaction_id'].astype(float)
Transaction_details['Transaction_id']=Transaction_details['Transaction_id'].astype(int)

print(Transaction_details)



# inserting data into tables

for row in Transactions.itertuples():
    cursor.execute('''
                INSERT INTO Transactions (Customer_id,Account_id,Acc_number,Transaction_type_code,Transaction_type_desc,Transaction_date)
                VALUES (?,?,?,?,?,?)
                ''',

                row.Customer_id,
                row.Account_id,
                row.Acc_number,
                row.Transaction_type_code,
                row.Transaction_type_desc,
                row.Transaction_date,

                )

for row in Cards.itertuples():
    cursor.execute('''
                    INSERT INTO Cards (Customer_id,Acc_number,Card_number,Maximum_limit,Expiry_Date,Credit_score)
                    VALUES (?,?,?,?,?,?)
                    ''',
                   row.Customer_id,
                   row.Acc_number,
                   row.Card_number,
                   row.Maximum_limit,
                   row.Expiry_Date,
                   row.Credit_score
                   )

for row in Transaction_details.itertuples():
    cursor.execute('''
                    INSERT INTO Transaction_details (Transaction_id,Transaction_Amount,Merchant_details,Acc_number)
                    VALUES (?,?,?,?)
                    ''',
                   row.Transaction_id,
                   row.Transaction_Amount,
                   row.Merchant_details,
                   row.Acc_number
                   )
cnxn.commit()