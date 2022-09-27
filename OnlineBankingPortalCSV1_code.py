import pyodbc
import pandas as pd

# Connection steps to the server
server = 'LAPTOP-SELQSNPH'
database = 'sai'
username = 'maram'
password = 'dima2k21'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()

# import data from csv
data = pd.read_csv(r'C:\Users\maram\PycharmProjects\pythonProject\OnlineBankingPortal_data_file1.csv')


# Branch table
Branch = pd.DataFrame(data, columns=['Branch_name', 'Branch_Address', 'IFSC_code'])
Branch_1 = pd.DataFrame(data, columns=['Branch_name', 'Branch_Address', 'IFSC_code'])
Branch = Branch.astype('str')
Branch['Branch_id'] = Branch.groupby(['Branch_name', 'Branch_Address', 'IFSC_code'], sort=False).ngroup() + 1

Branch_1['Branch_1_id'] = Branch_1.groupby(['Branch_name', 'Branch_Address', 'IFSC_code'], sort=False).ngroup() + 1
Branch = Branch.drop_duplicates(subset=None, keep="first", inplace=False)
print(Branch)

# Employee Table
Employee = pd.DataFrame(data, columns=['Emp_First_Name', 'Emp_Last_Name', 'Emp_Designation', 'previleges_granted'])
Employee = Employee.astype('str')
Employee['Emp_id'] = Employee.groupby(['Emp_First_Name', 'Emp_Last_Name'],sort=False).ngroup() + 1110  # gouping Emp_id by First and Last names starting from 1110

# dropping those columns from Employee table to create Emp_Previleges Table
Emp_Previleges = Employee.drop(['Emp_First_Name', 'Emp_Last_Name', 'Emp_Designation'], axis=1)

# removing duplicates
Employee = Employee.drop_duplicates(subset='Emp_id', keep="first", inplace=False)
print(Employee)
print(Emp_Previleges)

# Branch Employees table
Branch_Employees = pd.DataFrame(data, columns=['Emp_id', 'Branch_id', 'Start_Date', 'End_Date'])
Branch_Employees = Branch_Employees.astype('str')
Branch_Employees = Branch_Employees.drop_duplicates(subset=None, keep="first", inplace=False)
Branch_Employees['Emp_id'] = Employee['Emp_id']
Branch_Employees['Branch_id'] = Branch_1['Branch_1_id']
Branch_Employees['Start_Date'] = Branch_Employees['Start_Date'].astype('datetime64[ns]')
Branch_Employees['End_Date'] = Branch_Employees['End_Date'].astype('datetime64[ns]')
print(Branch_Employees)

# Inserting data into tables

for row in Branch.itertuples():
    cursor.execute('''
INSERT INTO Branch (Branch_name, Branch_address, IFSC_code)
VALUES (?,?,?)
''',
                   row.Branch_name,
                   row.Branch_Address,
                   row.IFSC_code,
                   )

for row in Employee.itertuples():
    cursor.execute('''
INSERT INTO Employee (Emp_First_Name, Emp_Last_Name,Emp_Designation)
VALUES (?,?,?)
''',
                   row.Emp_First_Name,
                   row.Emp_Last_Name,
                   row.Emp_Designation,
                   )

for row in Emp_Previleges.itertuples():
    cursor.execute('''
INSERT INTO Emp_Previleges (Emp_id,previleges_granted)
VALUES (?,?)
''',
                   row.Emp_id,
                   row.previleges_granted
                   )

for row in Branch_Employees.itertuples():
    cursor.execute('''
INSERT INTO Branch_Employees (Emp_id,Branch_id,Start_Date, End_Date)
VALUES (?,?,?,?)
''',
                   row.Emp_id,
                   row.Branch_id,
                   row.Start_Date,
                   row.End_Date,
                   )
cnxn.commit()
