import re,datetime
from datetime import datetime

import glob
import os
import random
import string
import requests
import pdfplumber
import pandas as pd
from sqlalchemy import create_engine
# import mysql.connector
# from mysql.connector import Error


def error_handle():
    keyword = None
    for row in rows:
        if "Leased Circuit Bill/Tax Invoice*" in row:
            keyword = "Leased Circuit Bill/Tax Invoice*"
            break
        elif "Credit Note" in row:
            keyword = "Credit Note"
            break

    # Print the result
    if keyword:
        print("Keyword found:", keyword)
    else:
        print("Keyword not found in the rows.")
    account_number = None

    for row in rows:
        match = re.search(r"Account Number (\d+)", row)
        if match:
            account_number_l = match.group(1)
            break

    if account_number_l:
        print("Account Number:", account_number_l)
    else:
        print("Account Number not found.")

    target_number = None

    for row in rows:
        match = re.search(r"[A-Z]+(\d+)", row)
        if match:
            target_number = match.group(0)
            break

    if target_number:
        print("Target Number:", target_number)
    else:
        print("Target Number not found.")

    invoice_date = None

    for row in rows:
        match = re.search(r"Invoice Date (\d{2}/\d{2}/\d{4})", row)
        if match:
            invoice_date= match.group(1)
            break
    if invoice_date:
        print("Invoice Date :", invoice_date)
    else:
        print("Invoice Date not found.")

    day, month, year = invoice_date.split('/')

    # Reformat the date components to the SQL date format "yy-mm-dd"
    invoice_date_sql = f"{year[-2:]}-{month}-{day}"

    print("Invoice Date (SQL format):", invoice_date_sql)
    data = {
            'Type' : [None],
            'Account_Number': account_number_l,
            'Credit_Note_Number': target_number ,
            'Credit_Issue_Date': invoice_date_sql ,
            'Customer_GSTIN' : [None] ,
            'Previous_Balance' : [None] ,
            'Amount_Payable' : [None],
            'MBPS': [None],
            'Adjustments' : [None],
            'Total_Charges_Rs' : [None],
            'Total_Charges' : [None],
            'Inv_No': [None],
            'Inv_No_Date': [None],
            'Minimum_Date' : [None],
            'Maximum_Date' : [None],
        }

    df = pd.DataFrame(data)
    print(df)

    connection_string = 'mysql+mysqlconnector://root:12345@localhost:3306/Bill'
    engine = create_engine(connection_string)

    # Export the DataFrame to a MySQL table
    table_name = 'intern1'
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

    # Close the database connection
    engine.dispose()

def process_credit_note():
    
    # amounts = rows[18].split()
    # print(amounts)
    target_row = None

    for row in rows:
        if re.match("^[-0-9. ]+$", row):
            amounts = row.split()
            if len(amounts) > 1:
                target_row = row
                break

    if target_row:
        print("Target Row:", target_row)
    else:
        print("No specific row found with multiple elements.")

    account_number = None

    for row in rows:
        match = re.search(r"Account Number (\d+)", row)
        if match:
            account_number_l = match.group(1)
            break

    if account_number_l:
        print("Account Number:", account_number_l)
    else:
        print("Account Number not found.")

    target_number = None

    for row in rows:
        match = re.search(r"[A-Z]+(\d+)", row)
        if match:
            target_number = match.group(0)
            break

    if target_number:
        print("Target Number:", target_number)
    else:
        print("Target Number not found.")

    invoice_date = None

    for row in rows:
        match = re.search(r"Credit Issue Date (\d{2}/\d{2}/\d{4})", row)
        if match:
            invoice_date = match.group(1)
            break

    if invoice_date:
        print("Invoice Date :", invoice_date)
    else:
        print("Invoice Date not found.")

    day, month, year = invoice_date.split('/')

    # Reformat the date components to the SQL date format "yy-mm-dd"
    invoice_date_sql = f"{year[-2:]}-{month}-{day}"

    print("Invoice Date (SQL format):", invoice_date_sql)

    target_number = None
    for row in rows:
        match = re.search(r"[A-Z]+(\d+)", row)
        if match:
            target_number = match.group(0)
            break

    if target_number:
        print("Target Number:", target_number)
    else:
        target_number="NAN"

    customer_gstin = None

    for row in rows:
        match = re.search(r"Customer GSTIN: ([A-Z0-9]+)", row)
        if match:
            customer_gstin = match.group(1)
            break

    if customer_gstin:
        print("Customer GSTIN:", customer_gstin)
    else:
        print("Customer GSTIN not found.")
        customer_gstin="NAN"

    amount_previous = float(amounts[0])
    print(amount_previous)

    amount_payable = float(amounts[-1])
    print(amount_payable)

    pattern = r"Adjustments (\d+(\.\d+)?)"
    amounts = []

    for row in rows:
        match = re.search(pattern, row)
        if match:
            amount = match.group(1)
            amounts.append(amount)

    # Print the amounts found
    print("Amounts after Adjustments:", amounts)

    pattern = r"Total Charges \(Rs.\) (\d+(?:\.\d+)?)"

    total_charges1 = []

    for row in rows:
        match = re.search(pattern, row)
        if match:
            total_charge = match.group(1)
            total_charges1.append(total_charge)

    # Print the extracted numeric values
    for total_charge in total_charges1:
        print(total_charge)

    pattern = r"Total Charges (\d+(?:\.\d+)?)"

    total_charges = []

    for row in rows:
        match = re.search(pattern, row)
        if match:
            total_charge = match.group(1)
            total_charges.append(total_charge)

    # Print the extracted numeric values
    for total_charge in total_charges:
        print(total_charge)

    pattern = r":Inv\.No-(.*)"
    occurrences = []

    for row in rows:
        match = re.search(pattern, row)
        if match:
            occurrence = match.group(1)
            # Truncate everything if any amount is found
            if re.search(r"\$\d+(\.\d+)?", occurrence):
                occurrence = ""
            occurrences.append(occurrence)

        # Split the occurrence list and remove the -1 index
    splitted_occurrences = [occurrence.split()[:-1] for occurrence in occurrences]


    splitted_occurrences_first = splitted_occurrences[0][0]
    splitted_occurrences_second = splitted_occurrences[0][1]
        # Print the splitted occurrences starting with ":Inv.No-" with amounts truncated
    for occurrence in splitted_occurrences[0]:
        print(occurrence)


    data = {
            'Type' : keyword,
            'Account_Number': account_number_l ,
            'Credit_Note_Number': target_number ,
            'Credit_Issue_Date': invoice_date_sql ,
            'Customer_GSTIN' : customer_gstin ,
            'Previous_Balance' : amount_previous,
            'Amount_Payable' : amount_payable,
            'MBPS': None,
            'Adjustments' : amounts,
            'Total_Charges_Rs' : total_charges1,
            'Total_Charges' : total_charges,
            'Inv_No': splitted_occurrences_first,
            'Inv_No_Date': splitted_occurrences_second,
            'Minimum_Date' : None,
            'Maximum_Date' : None,
        }

    df=pd.DataFrame(data)
    print(df)

    connection_string = 'mysql+mysqlconnector://root:12345@localhost:3306/Bill'
    engine = create_engine(connection_string)

    # Export the DataFrame to a MySQL table
    table_name = 'intern1'
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

    # Close the database connection
    engine.dispose()

def leased_circuit_bill():
    target_row = None

    for row in rows:
        if re.match("^[-0-9. ]+$", row):
            target_row = row
            break

    if target_row:
        print("Target Row:", target_row)
    else:
        print("No specific row found with all numbers.")

    amounts = target_row.split()
    print(amounts)

    # account_number = rows[3].split()
    # print(account_number[-1])
    account_number = None

    for row in rows:
        match = re.search(r"Account Number (\d+)", row)
        if match:
            account_number_l = match.group(1)
            break

    if account_number_l:
        print("Account Number:", account_number_l)
    else:
        print("Account Number not found.")

    target_number = None

    for row in rows:
        match = re.search(r"[A-Z]+(\d+)", row)
        if match:
            target_number = match.group(0)
            break

    if target_number:
        print("Target Number:", target_number)
    else:
        print("Target Number not found.")

    invoice_date = None

    for row in rows:
        match = re.search(r"Invoice Date (\d{2}/\d{2}/\d{4})", row)
        if match:
            invoice_date= match.group(1)
            break
    if invoice_date:
        print("Invoice Date :", invoice_date)
    else:
        print("Invoice Date not found.")

    day, month, year = invoice_date.split('/')

    # Reformat the date components to the SQL date format "yy-mm-dd"
    invoice_date_sql = f"{year[-2:]}-{month}-{day}"

    print("Invoice Date (SQL format):", invoice_date_sql)

    customer_gstin = None

    for row in rows:
        match = re.search(r"Customer GSTIN: ([A-Z0-9]+)", row)
        if match:
            customer_gstin = match.group(1)
            break

    if customer_gstin:
        print("Customer GSTIN:", customer_gstin)
    else:
        print("Customer GSTIN not found.")

    amount_previous = float(amounts[0])
    print(amount_previous)

    amount_payable = float(amounts[-1])
    print(amount_payable)

    pattern = r"Circuit Type :.*?(\d+(\.\d+)?) MBPS"
    numbers = []

    for row in rows:
        if "Circuit Type" in row:
            match = re.search(pattern, row)
            if match:
                number = match.group(1)
                numbers.append(number)

    # Print the numbers found
    print("Numbers before MBPS in rows with 'Circuit Type':", numbers)

    pattern = r"Adjustments (\d+(\.\d+)?)"
    amounts = []

    found_adjustments = False

    for row in rows:
        match = re.search(pattern, row)
        if match:
            amount = match.group(1)
            amounts.append(amount)
            found_adjustments = True

    if not found_adjustments:
        amounts.append('0')

    # Print the amounts found
    print("Amounts after Adjustments:", amounts)

    total_charges = None

    for row in rows:
        match = re.search(r"Total Charges \(Rs.\) (\d+(\.\d+)?)", row)
        if match:
            total_charges1 = match.group(1)
            break

    if total_charges1:
        print("Total Charges (Rs.):", total_charges1)
    else:
        print("Total Charges (Rs.) not found.")

    pattern = r"Total Charges (\d+(?:\.\d+)?)"

    total_charges = []

    for row in rows:
        match = re.search(pattern, row)
        if match:
            total_charge = match.group(1)
            total_charges.append(total_charge)

    # Print the extracted numeric values
    for total_charge in total_charges:
        print(total_charge)

    regex_pattern = r"Lead A/Bill to Address:- Lead B Address:-\s*(.*?)\s*One Time Charges"

    match = re.search(regex_pattern, text, re.IGNORECASE | re.DOTALL)
    if match:
        extracted_row = match.group(1).strip()
        extracted_row_without_charges = re.sub(r"\bRecurring Charges\s*[-0-9.]+\s*", "", extracted_row)
        print("Extracted Row:", extracted_row_without_charges)
    else:
        print("No match found.")


    date_pattern = r'\d{2}/\d{2}/\d{2}'

    periods = []

    for row in rows:
        matches = re.findall(date_pattern + r'\s+to\s+' + date_pattern, row)
        periods.extend(matches)

    print(periods)

    min_dates = []
    max_dates = []

    for date_range in periods:
        start_date, end_date = date_range.split(" to ")
        start_date = datetime.strptime(start_date, "%d/%m/%y")
        end_date = datetime.strptime(end_date, "%d/%m/%y")
        min_dates.append(start_date)
        max_dates.append(end_date)

    min_date = min(min_dates)
    max_date = max(max_dates)

    # min_date_str = min_date.strftime("%d/%m/%y")
    # max_date_str = max_date.strftime("%d/%m/%y")

    min_date_str = min_date.strftime("%y-%m-%d")
    max_date_str = max_date.strftime("%y-%m-%d")

    print("Minimum date before 'to':", min_date_str)
    print("Maximum date after 'to':", max_date_str)


    data = {
            'Type' : keyword,
            'Account_Number': account_number_l ,
            'Credit_Note_Number':  target_number,
            'Credit_Issue_Date': invoice_date_sql ,
            'Customer_GSTIN' : customer_gstin ,
            'Previous_Balance' : amount_previous,
            'Amount_Payable' : amount_payable,
            'MBPS' : numbers,
            'Adjustments' : amounts,
            'Total_Charges_Rs' : total_charges1,
            'Total_Charges' : total_charges,
            'Inv_No': None,
            'Inv_No_Date': None,
            'Minimum_Date' : min_date_str,
            'Maximum_Date' : max_date_str,
        }

    df=pd.DataFrame(data)
    print(df)

    connection_string = 'mysql+mysqlconnector://root:12345@localhost:3306/Bill'
    engine = create_engine(connection_string)

    # Export the DataFrame to a MySQL table
    table_name = 'intern1'
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

    # Close the database connection
    engine.dispose()

from logging import NullHandler

def download_file(url):
    local_filename = url.split('/')[-1]

    with requests.get(url) as r:
        with open(local_filename, 'wb') as f:
            f.write(r.content)

    return local_filename

folder_path = "C:/Users/Lenovo/Desktop/internship/testing_on_all"  # Replace with the actual folder path

# Iterate through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".pdf"):
        invoice_nm = os.path.join(folder_path, filename)
        invoice = invoice_nm.replace("\\", "/")
        print(invoice)
        with pdfplumber.open(invoice) as pdf:
            page = pdf.pages[0]
            text = page.extract_text()

        # print(text)
        rows = text.split('\n')

        target_number = None

        for row in rows:
            match = re.search(r"[A-Z]+(\d+)", row)
            if match:
                target_number = match.group(0)
                break
        keyword = None
        for row in rows:
            if "Leased Circuit Bill/Tax Invoice*" in row:
                keyword = "Leased"
                if target_number and len(target_number) >= 6 and target_number[5] == "C":
                    print("Error: 6th occurrence of target_number contains 'C'.")
                    # Additional error handling or logic can be added here
                    error_handle()
                else:
                    leased_circuit_bill()
            elif "Credit Note" in row:
                keyword = "Credit"
                process_credit_note() 
                break

