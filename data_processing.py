import pandas as pd

def process_data_zuba(df_zuba):    
    # Clean column names by removing leading/trailing spaces
    df_zuba.columns = df_zuba.columns.str.strip()
    df_zuba = df_zuba.drop('Payment Method', axis=1) #drop payment Method to avoid merge issue 

    # Make sure Booking No and Confiramtion Code are set to string 
    df_zuba['Booking No.'] = df_zuba['Booking No.'].astype(str)
    df_zuba['Confirmation Code'] = df_zuba['Confirmation Code'].astype(str)

    df_zuba_result = df_zuba

    return df_zuba_result


def process_data_ipay(df_ipay):
    # Clean column names by removing leading/trailing spaces
    df_ipay.columns = df_ipay.columns.str.strip()

    # Retain only the chosen columns
    chosen_columns = ['Merchant RefNo', 'Payment Method', 'Card type', 'Payment Category']  # Define the columns you want to keep
    df_ipay_result = df_ipay[chosen_columns]  # Filter the DataFrame to keep only these columns

    return df_ipay_result

def merge_dataset(zuba_result_df, ipay_result_df):
    
    # Perform a left join
    df_zuba = pd.merge(ipay_result_df, zuba_result_df, left_on='Merchant RefNo', right_on='Booking No.', how='left')

    # Column formatting for easier calculation 
    df_zuba["Room / Per Night / Price/每晚/價格"] = pd.to_numeric(df_zuba["Room / Per Night / Price/每晚/價格"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Room / Cleaning Fee/清潔費"] = pd.to_numeric(df_zuba["Room / Cleaning Fee/清潔費"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Sales Tax"] = pd.to_numeric(df_zuba["Sales Tax"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Hotel Tax"] = pd.to_numeric(df_zuba["Hotel Tax"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Service Tax"] = pd.to_numeric(df_zuba["Service Tax"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Tourist Tax"] = pd.to_numeric(df_zuba["Tourist Tax"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Day(s)"] = pd.to_numeric(df_zuba["Day(s)"], errors='coerce')
    df_zuba["Total Amount"] = pd.to_numeric(df_zuba["Total Amount"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba['Zuba Bank Received Payment (RM)'] = None

    #calculate for MDR value 
    def assign_mdr_value(value):

        #To check 2 columns
        payment_method = value['Payment Method']
        card_type = value['Card type']
        payment_category = value['Payment Category']

        if payment_method in ['AliPay', 'AliPay_RMB']:
            return '3.00%'
        elif payment_method in ['Boost Wallet', 'MCash', 'ShopeePay', 'UnionPay Online QR (MYR)']:
            return '1.50%'
        elif payment_method in ['Credit Card', 'UnionPay Credit Card', 'MyDebit']:
            if card_type in ['LOCAL CREDIT', 'LOCAL DEBIT']:
                return '2.40%'
            elif card_type in ['INTL. DEBIT', 'INTL. CREDIT']:
                return '4.00%'
        elif payment_method in ['FPX', 'FPX_Affin', 'FPX_Agro', 'FPX_ALB', 'FPX_Ambank', 'FPX_BIMB', 'FPX_BOC', 'FPX_BRakyat', 'FPX_BSN', 'FPX_CIMB', 'FPX_HLB', 'FPX_HSBC', 'FPX_KFH', 'FPX_M2U', 'FPX_Muamalat', 'FPX_OCBC', 'FPX_PBB', 'FPX_RHB', 'FPX_SCB', 'FPX_UOB']:
            return '2.40% / RM0.60'
        elif payment_method in ['GrabPay']:
            if payment_category in ['Grab PayLater Postpaid']:
                return '6.00%'
            else:
                return '1.70%'
        elif payment_method in ['TNGWalletOnline']:
            return '1.70%'
        elif payment_method in ['Paypal']:
            return '4.30% + RM2.00'
        elif payment_method in ['MAE by Maybank2u', 'DuitNowQROnline']:
            return '1.00%'
        else:   
            return None

    df_zuba["ipay88 MDR"] = df_zuba.apply(assign_mdr_value, axis=1)
    
    # Add calculation and new column 
    df_zuba["Total Room Rate"] = df_zuba["Room / Per Night / Price/每晚/價格"] * df_zuba["Day(s)"]

    # Define the function to assign TA commission
    def assign_TA_com(row):
        if row["User"] == 'Guests':  # Check the "User" column for "Guest"
            return None  # No commission for "Guest"
        else:
            return row["Total Room Rate"] * 10 / 100  # Calculate 10% commission

    # Apply the function row by row
    df_zuba['TA Commission - 10% (RM)'] = df_zuba.apply(assign_TA_com, axis=1)
    
    df_zuba['Host Commission - 12% (RM)'] = df_zuba["Total Room Rate"] * 12/100
    df_zuba['Total Amount After Commission (RM)'] = df_zuba["Total Room Rate"] - df_zuba['Host Commission - 12% (RM)']
    df_zuba['iPay88 Received Amount (RM)'] = df_zuba['Total Amount']

    # Calculate for Zuba REceived Payment with iPay88 rate 
    def calculate_zuba_received_payment(row):

        #To check 2 columns
        payment_method = row['Payment Method']
        card_type = row['Card type']
        payment_category = row['Payment Category']

        if payment_method in ['AliPay', 'AliPay_RMB']:
            return round(row['iPay88 Received Amount (RM)'] - (row['iPay88 Received Amount (RM)'] * 0.03),2)
        elif payment_method in ['Boost Wallet', 'MCash', 'ShopeePay', 'UnionPay Online QR (MYR)']:
            return round(row['iPay88 Received Amount (RM)'] - (row['iPay88 Received Amount (RM)'] * 0.015),2)
        elif payment_method in ['MAE by Maybank2u', 'DuitNowQROnline']:
            return round(row['iPay88 Received Amount (RM)'] - (row['iPay88 Received Amount (RM)'] * 0.01),2)
        elif payment_method in ['Credit Card', 'UnionPay Credit Card', 'MyDebit']:
            if card_type in ['LOCAL CREDIT', 'LOCAL DEBIT']:
                return round(row['iPay88 Received Amount (RM)'] - (row['iPay88 Received Amount (RM)'] * 0.024),2)
            elif card_type in ['INTL. DEBIT', 'INTL. CREDIT']:
                return round(row['iPay88 Received Amount (RM)'] - (row['iPay88 Received Amount (RM)'] * 0.04),2)
        elif payment_method in ['FPX', 'FPX_Affin', 'FPX_Agro', 'FPX_ALB', 'FPX_Ambank', 'FPX_BIMB', 'FPX_BOC', 'FPX_BRakyat', 'FPX_BSN', 'FPX_CIMB', 'FPX_HLB', 'FPX_HSBC', 'FPX_KFH', 'FPX_M2U', 'FPX_Muamalat', 'FPX_OCBC', 'FPX_PBB', 'FPX_RHB', 'FPX_SCB', 'FPX_UOB']:
            amount_after_deduction = row['iPay88 Received Amount (RM)'] - (row['iPay88 Received Amount (RM)'] * 0.024)
            return round(amount_after_deduction if amount_after_deduction > 0.6 else (row['iPay88 Received Amount (RM)'] - 0.6), 2)
        elif payment_method in ['GrabPay']:
            if payment_category in ['Grab PayLater Postpaid']:
                return round(row['iPay88 Received Amount (RM)'] - (row['iPay88 Received Amount (RM)'] * 0.06), 2)
            else: 
                return round(row['iPay88 Received Amount (RM)'] - (row['iPay88 Received Amount (RM)'] * 0.017), 2)
        elif payment_method in ['TNGWalletOnline']:
            return round(row['iPay88 Received Amount (RM)'] - (row['iPay88 Received Amount (RM)'] * 0.017), 2)
        elif payment_method in ['Paypal']:
            return round(row['iPay88 Received Amount (RM)'] - (row['iPay88 Received Amount (RM)'] * 0.043 + 2), 2)
        else:
            return None

    # Apply the function to each row
    df_zuba['Zuba Bank Received Payment (RM)'] = df_zuba.apply(calculate_zuba_received_payment, axis=1)

    #Calculate for Total Amount Pay to Host (RM), use Zuba receive payment because already include tax inside 
    df_zuba['Total Amount Pay to Host (RM)'] = df_zuba['Zuba Bank Received Payment (RM)'] - df_zuba['Host Commission - 12% (RM)'] - df_zuba['Tourist Tax']

    #Reorder Column 
    report_order = ['Booking No.', 'Confirmation Code', 'Booking Date', 'User', 'Booker Name', 'Booker Email', 'Check-in Date', 'Check-out Date', 'Property', 'Owner Email', 'Room Type',  
                    'Unit(s)', 'Total Guest', 'Room / Per Night / Price/每晚/價格', 'Day(s)', 'Total Room Rate', 'Room / Cleaning Fee/清潔費', 'Sales Tax', 'Hotel Tax', 'Service Tax', 'Tourist Tax', 'Total Amount', 'iPay88 Received Amount (RM)',
                    'ipay88 MDR', 'Zuba Bank Received Payment (RM)', 'Payment Method', 'Status', 'TA Commission - 10% (RM)', 'Host Commission - 12% (RM)', 'Total Amount After Commission (RM)', 'Total Amount Pay to Host (RM)'] 

    df_zuba = df_zuba[report_order]

    #Change column name 
    df_zuba.rename(columns={'Room / Per Night / Price/每晚/價格': 'Room / Per Night / Price/每晚/價格 (RM)'}, inplace=True)
    df_zuba.rename(columns={'Room / Cleaning Fee/清潔費': 'Room / Cleaning Fee/清潔費 (RM)'}, inplace=True)
    df_zuba.rename(columns={'Sales Tax': 'Sales & Service Tax (RM)'}, inplace=True)
    df_zuba.rename(columns={'Service Tax': 'Service Charge (RM)'}, inplace=True)
    df_zuba.rename(columns={'Tourist Tax': 'Tourist Tax (RM)'}, inplace=True)
    df_zuba.rename(columns={'Hotel Tax': 'Hotel Tax (RM)'}, inplace=True)
    df_zuba.rename(columns={'Total Room Rate': 'Total Room Rate (RM)'}, inplace=True)
    df_zuba.rename(columns={'Total Amount': 'Total Amount (RM)'}, inplace=True)
    
    #   the DataFrame by the 'Room Type' column
    # df_zuba = df_zuba.sort_values(by='Room Type', ascending=True)

    # Group by "Owner Email" and calculate the sum 
    total_host = df_zuba.groupby('Owner Email')['Total Amount Pay to Host (RM)'].sum().reset_index()
    total_com = df_zuba.groupby('Owner Email')['Total Amount After Commission (RM)'].sum().reset_index()

    df_zuba = df_zuba.sort_values(by='Booking No.', ascending=True)

    # Prepare a list to store the output DataFrame with totals rows
    output = []

    # Group by "Owner Email" and add totals row under each group
    for owner_email, group in df_zuba.groupby('Owner Email'):
        output.append(group)  # Add the original group data
        # Create a totals row with NaN for non-relevant columns
        total_row = pd.DataFrame(
            {
                'Booking No.': ['Total'],
                'Total Amount Pay to Host (RM)': [total_host.loc[total_host['Owner Email'] == owner_email, 'Total Amount Pay to Host (RM)'].values[0]],
                'Total Amount After Commission (RM)': [total_com.loc[total_com['Owner Email'] == owner_email, 'Total Amount After Commission (RM)'].values[0]]
            }
        )

        empty_row = pd.DataFrame(
            {
                'Owner Email': [''],
            }
        )

        # Add rows 
        output.append(total_row)  
        output.append(empty_row)

    # Combine everything into a single DataFrame
    df_zuba_result = pd.concat(output, ignore_index=True)

    # Add "Created By" and "Checked By" rows at the bottom
    created_checked_rows = pd.DataFrame([
        {'Booking No.': ''},
        {'Booking No.': ''},
        {'Booking No.': ''},
        {'Booking No.': ''},
        {'Booking No.': ''},
        {'Booking No.': ''},
        {'Booking No.': ''},
        {'Booking No.': 'Created By:', 'Confirmation Code': 'Marcus Chin'},
        {'Booking No.': 'Checked By:'}
    ])
    df_zuba_result = pd.concat([df_zuba_result, created_checked_rows], ignore_index=True)

    return df_zuba_result