import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2
from sqlalchemy import create_engine
import os
from datetime import datetime
sns.set_theme(style="whitegrid")

customer_sales_fact_table=None
fulfilment_df=None
inventorysales_df=None
date_dimension_table=None
customers=None
products=None
payments=None
orders=None
orderdetails=None
offices=None
employees=None
sales_df=None
pricing_analysis_df=None

def extract_and_transform():
    print('extract and transform starts@'+str(datetime.now()))
    global customer_sales_fact_table
    global fulfilment_df
    global inventorysales_df
    global date_dimension_table
    global customers
    global products
    global payments
    global orders
    global orderdetails
    global offices
    global employees
    global sales_df
    global pricing_analysis_df
    def custom_aggregation1(group):
        quantityInStock=group['quantityInStock'].iloc[0]
        quantityOrdered=group['quantityOrdered'].sum()
        return pd.Series({'quantityInStock': quantityInStock, 'quantityOrdered': quantityOrdered})

    def custom_aggregation2(group):
        orderNumber=group['orderNumber'].iloc[0]
        revenue=group['revenue'].sum()
        return pd.Series({'orderNumber': orderNumber, 'revenue': revenue})
    
    def custom_aggregation3(group):
        amount=group['amount'].sum()
        officeCode=group['officeCode'].iloc[0]
        return pd.Series({'amount': amount, 'officeCode': officeCode})
    
    def custom_aggregation4(group):
        priceEach=group['priceEach'].mean()
        quantityOrdered=group['quantityOrdered'].iloc[0]
        return pd.Series({'priceEach': priceEach, 'quantityOrdered': quantityOrdered})

    products = pd.read_csv('Products.txt', sep=",", header=None,encoding="ISO-8859-1")
    payments = pd.read_csv('Payments.txt', sep=",", header=None,encoding="ISO-8859-1")
    orders = pd.read_csv('Orders.txt', sep=",", header=None,encoding="ISO-8859-1")
    orderdetails = pd.read_csv('OrderDetails.txt', sep=",", header=None,encoding="ISO-8859-1")
    offices = pd.read_csv('Offices.txt', sep=",", header=None,encoding="ISO-8859-1")
    employees = pd.read_csv('Employees.txt', sep=",", header=None,encoding="ISO-8859-1")
    customers = pd.read_csv('Customers.txt', sep=",", header=None,encoding="ISO-8859-1")

    product_headers=['productCode','productName','productLine','productScale','productVendor','productDescription','quantityInStock','buyPrice','MSRP']
    products.columns=product_headers
    payments_headers=['customerNumber','checkNumber','paymentDate','amount']
    payments.columns=payments_headers
    orders_columns=['orderNumber','orderDate','requiredDate','shippedDate','status','comments','customerNumber']
    orders.columns=orders_columns
    orderdetails_columns=['orderNumber','productCode','quantityOrdered','priceEach','orderLineNumber']
    orderdetails.columns=orderdetails_columns
    offices_columns=['officeCode','city','phone','addressLine1','addressLine2','state','country','postalCode','territory']
    offices.columns=offices_columns
    offices.drop(columns=['addressLine2','state'], inplace=True)
    offices.fillna(value={'territory': 'NA'}, inplace=True)
    employees_columns=['employeeNumber','lastName','firstName','extension','email','officeCode','reportsTo','jobTitle']
    employees.columns=employees_columns
    customers_columns=['customerNumber','customerName','contactLastName','contactFirstName','phone','addressLine1','addressLine2','city','state','postalCode','country','salesRepEmployeeNumber','creditLimit']
    customers.columns=customers_columns
    customers.drop(columns=['addressLine2','state','postalCode'], inplace=True)

    start_date = '2003-01-01'
    end_date = '2005-07-01'

    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    date_dimension_table = pd.DataFrame({
        'dateKey': range(1, len(date_range) + 1),
        'date': date_range
    })

    date_dimension_table['year'] = date_dimension_table['date'].dt.year
    date_dimension_table['month'] = date_dimension_table['date'].dt.month
    date_dimension_table['day'] = date_dimension_table['date'].dt.day
    date_dimension_table['dayOfWeek'] = date_dimension_table['date'].dt.dayofweek
    date_dimension_table['dayName'] = date_dimension_table['date'].dt.day_name()
    date_dimension_table['quarter'] = date_dimension_table['date'].dt.quarter
    date_dimension_table['weekOfYear'] = date_dimension_table['date'].dt.isocalendar().week

    merged_df = pd.merge(orderdetails,products,on='productCode',how='inner')
    merged_df = pd.merge(merged_df,orders,on='orderNumber',how='inner')
    inventorysales_df = merged_df[['productCode', 'quantityInStock', 'quantityOrdered']]
    inventorysales_df = inventorysales_df.groupby(['productCode']).apply(custom_aggregation1).reset_index()

    fulfilment_df = pd.merge(orders, orderdetails, on='orderNumber', how='inner')
    fulfilment_df['orderDate'] = pd.to_datetime(fulfilment_df['orderDate'])
    fulfilment_df['shippedDate'] = pd.to_datetime(fulfilment_df['shippedDate'])
    fulfilment_df['daysToShip'] = (fulfilment_df['shippedDate'] - fulfilment_df['orderDate']).dt.days
    fulfilment_df = fulfilment_df[['orderNumber', 'productCode', 'daysToShip', 'status']]
    fulfilment_df.rename(columns={'status': 'shipmentStatus'}, inplace=True)

    customer_sales_df = pd.merge(orders, orderdetails, on='orderNumber', how='inner')
    customer_sales_df = pd.merge(customer_sales_df, customers, on='customerNumber', how='inner')
    customer_sales_df['revenue'] = customer_sales_df['priceEach'] * customer_sales_df['quantityOrdered']
    customer_sales_df['orderDate'] = pd.to_datetime(customer_sales_df['orderDate'])
    customer_sales_df = pd.merge(customer_sales_df, date_dimension_table, left_on='orderDate', right_on='date', how='inner')
    customer_sales_df.drop(columns=['date'], inplace=True)
    customer_sales_fact_table = customer_sales_df[['customerNumber', 'orderNumber', 'dateKey', 'revenue']]
    customer_sales_fact_table = customer_sales_fact_table.groupby(['customerNumber','dateKey']).apply(custom_aggregation2).reset_index()
    customer_sales_fact_table['orderNumber'] = customer_sales_fact_table['orderNumber'].astype(int)

    sales_df = pd.merge(payments, customers, on='customerNumber')
    sales_df = pd.merge(sales_df, employees, left_on='salesRepEmployeeNumber', right_on='employeeNumber')
    sales_df=sales_df[['employeeNumber','amount','officeCode']]
    sales_df = sales_df.groupby(['employeeNumber']).apply(custom_aggregation3).reset_index()

    pricing_analysis_df = pd.merge(products[['productCode', 'MSRP']], orderdetails[['productCode', 'quantityOrdered', 'priceEach']], on='productCode')
    pricing_analysis_df = pricing_analysis_df.groupby(['productCode','MSRP']).apply(custom_aggregation4).reset_index()
    print('extract and transform ends@'+str(datetime.now()))


def load():
    print('load starts@'+str(datetime.now()))
    postgres_host = 'postgres'
    postgres_db = 'ClassicModels'
    postgres_user = 'postgres'
    postgres_password = 'postgres'
    postgres_port = '5432'
    db_config = {
        'host':postgres_host if postgres_host!=None else 'localhost',
        'database':postgres_db if postgres_db!=None else 'ClassicModels',
        'user':postgres_user if postgres_user!=None else 'postgres',
        'password':postgres_password if postgres_password!=None else 'root',
        'port':postgres_port if postgres_port!=None else '5432'
    }
    print(db_config)
    while True:
        try:
            connection = psycopg2.connect(**db_config)
            engine = create_engine('postgresql+psycopg2://', connect_args=db_config)
            if engine:
                break
        except Exception as e:
            print('----------- Cannot connect to database due to \n'+ str(e))
    start_time = datetime.now()
    print('load starts@'+str(start_time))
    customer_sales_fact_table.to_sql('customer_sales', engine, index=False, if_exists='replace')
    fulfilment_df.to_sql('fulfilment', engine, index=False, if_exists='replace')
    inventorysales_df.to_sql('inventorysales', engine, index=False, if_exists='replace')
    date_dimension_table.to_sql('date_dimension', engine, index=False, if_exists='replace')
    customers.to_sql('customers', engine, index=False, if_exists='replace')
    products.to_sql('products', engine, index=False, if_exists='replace')
    payments.to_sql('payments', engine, index=False, if_exists='replace')
    orders.to_sql('orders', engine, index=False, if_exists='replace')
    orderdetails.to_sql('orderdetails', engine, index=False, if_exists='replace')
    offices.to_sql('offices', engine, index=False, if_exists='replace')
    employees.to_sql('employees', engine, index=False, if_exists='replace')
    sales_df.to_sql('salesRepPerformance', engine, index=False, if_exists='replace')
    pricing_analysis_df.to_sql('pricing_analysis', engine, index=False, if_exists='replace')

    end_time = datetime.now()
    timelog = {'timestamp': [start_time, end_time]}
    timelog_df = pd.DataFrame(timelog, index=['process_start', 'process_end'])
    timelog_df.to_sql('timelog', engine, index=False, if_exists='replace')
    engine.dispose()
    connection.close()
    print('load ends@'+str(end_time))

extract_and_transform()
load()