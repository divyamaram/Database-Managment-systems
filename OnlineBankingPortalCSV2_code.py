import pyodbc
import pandas as pd

# Connection steps to the server
from OnlineBankingPortalCSV1_code import Branch, Branch_1

server = 'LAPTOP-SELQSNPH'
database = 'sai'
username = 'maram'
password = 'dima2k21'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()

# import data from csv
data = pd.read_csv(r'C:\Users\maram\PycharmProjects\pythonProject\OnlineBankingPortal_data_file2.csv')

# Customer table
Customer = pd.DataFrame(data,columns=['First_Name', 'Last_Name', 'Date_of_birth', 'Gender', 'Phone_number', 'SSN', 'Email','Address'])
Customer_1 = pd.DataFrame(data,columns=['First_Name', 'Last_Name', 'Date_of_birth', 'Gender', 'Phone_number', 'SSN', 'Email','Address'])
Customer = Customer.astype('str')
Customer['Customer_id'] = Customer.groupby(['First_Name', 'Last_Name'], sort=False).ngroup() + 100
Customer_1['Customer_1_id'] = Customer_1.groupby(['First_Name', 'Last_Name'], sort=False).ngroup() + 100

# removing duplicates
Customer = Customer.drop_duplicates(subset=None, keep="first", inplace=False)

print(Customer)

# Account table
Accounts = pd.DataFrame(data,columns=['Account_id', 'Customer_id', 'Acc_number', 'Account_type_code', 'Account_type_desc','Minimum_balance', 'Account_balance', 'Branch_name', 'Branch_id', 'Date_Opened'])
Accounts_1 = pd.DataFrame(data,columns=['Account_id', 'Customer_id', 'Acc_number', 'Account_type_code', 'Account_type_desc','Minimum_balance', 'Account_balance', 'Branch_name', 'Branch_id', 'Date_Opened'])
Accounts = Accounts.astype('str')
Accounts['Account_id'] = Accounts.groupby(['Acc_number'], sort=False).ngroup() + 200

# to get repeating values
Accounts_1['Account_1_id'] = Accounts_1.groupby(['Acc_number'], sort=False).ngroup() + 200
Accounts['Branch_id'] = Branch_1['Branch_1_id']
Accounts['Customer_id'] = Customer_1['Customer_1_id']

# Customer['Customer_id'] = Customer.groupby(['First_Name','Last_Name'],sort=False).ngroup()+100
Accounts['Date_Opened'] = Accounts['Date_Opened'].astype('datetime64[ns]')

print(Accounts)

Branch_1['Branch_1_id'] = Branch_1.groupby(['Branch_name', 'Branch_Address', 'IFSC_code'], sort=False).ngroup() + 1
Branch = Branch.drop_duplicates(subset=None, keep="first", inplace=False)

#Account_types table
Account_Types= pd.DataFrame(data, columns= ['Account_type_desc','Account_type_code','Acc_number'])
Account_Types = Account_Types.astype('str')

Merge_Account_Types_Accounts=pd.merge(Account_Types,Accounts,on='Acc_number')
Account_Types['Account_id']=Merge_Account_Types_Accounts.Account_id

print(Account_Types)

# Inserting data into tables

for row in Customer.itertuples():
    cursor.execute('''
                    INSERT INTO Customer (First_Name,Last_Name,Date_of_birth,Gender,Phone_number,SSN,Email,Address)
                    VALUES (?,?,?,?,?,?,?,?)
                    ''',
                   row.First_Name,
                   row.Last_Name,
                   row.Date_of_birth,
                   row.Gender,
                   row.Phone_number,
                   row.SSN,
                   row.Email,
                   row.Address,
                   )

for row in Accounts.itertuples():
    cursor.execute('''
                    INSERT INTO Accounts (Customer_id,Acc_number,Account_type_code,Minimum_balance,Account_balance,Branch_id,Date_Opened)
                    VALUES (?,?,?,?,?,?,?)
                    ''',
                   row.Customer_id,
                   row.Acc_number,
                   row.Account_type_code,
                   row.Minimum_balance,
                   row.Account_balance,
                   row.Branch_id,
                   row.Date_Opened,
                   )
for row in Account_Types.itertuples():
    cursor.execute('''
                   INSERT INTO Account_Types (Account_id,Account_type_desc,Account_type_code)
                    VALUES (?,?,?)
                    ''',
                   row.Account_id,
                   row.Account_type_desc,
                   row.Account_type_code,
                  )
cnxn.commit()