# data_processing.py

import pandas as pd

def process_data_zuba(df_zuba):    
    # Clean column names by removing leading/trailing spaces
    df_zuba.columns = df_zuba.columns.str.strip()
    df_zuba_result = df_zuba

    return df_zuba_result


def process_data_ipay(df_ipay):
    # Clean column names by removing leading/trailing spaces
    df_ipay.columns = df_ipay.columns.str.strip()

    # Retain only the chosen columns
    chosen_columns = ['Merchant RefNo', 'Payment Method']  # Define the columns you want to keep
    df_ipay_result = df_ipay[chosen_columns]  # Filter the DataFrame to keep only these columns

    return df_ipay_result

def merge_dataset(zuba_result_df, ipay_result_df):
    
    # Perform a left join
    df_zuba = pd.merge(ipay_result_df, zuba_result_df, left_on='Merchant RefNo', right_on='Booking No.', how='left')

    # Column formatting for easier calculation 
    df_zuba["Room / Per Night / Price/每晚/價格"] = pd.to_numeric(df_zuba["Room / Per Night / Price/每晚/價格"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Room / Cleaning Fee/清潔費"] = pd.to_numeric(df_zuba["Room / Cleaning Fee/清潔費"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Room / Service Fee/服務費"] = pd.to_numeric(df_zuba["Room / Service Fee/服務費"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Room / Tax/稅金"] = pd.to_numeric(df_zuba["Room / Tax/稅金"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Day(s)"] = pd.to_numeric(df_zuba["Day(s)"].str.replace('天', '').str.strip(), errors='coerce')
    df_zuba["ipay88 MDR"] = None

    # Calculate "Total Room Rate" and add it as a new column
    df_zuba["Total Room Rate"] = df_zuba["Room / Per Night / Price/每晚/價格"] * df_zuba["Day(s)"]
    df_zuba['TA Commission - 10% (RM)'] = df_zuba["Total Room Rate"] * 10/100
    df_zuba['Host Commission - 12% (RM)'] = df_zuba["Total Room Rate"] * 12/100
    df_zuba['Total Amount After Commission (RM)'] = df_zuba["Total Room Rate"] - df_zuba['Host Commission - 12% (RM)']
    df_zuba['Total Amount Pay to Host (RM)'] = df_zuba["Room / Cleaning Fee/清潔費"] + df_zuba["Room / Service Fee/服務費"] + df_zuba["Room / Tax/稅金"] + df_zuba['Total Amount After Commission (RM)']

    #Reorder Column 
    report_order = ['Booking No.', 'Confirmation Code', 'Booking Date', 'User', 'Booker Name', 'Booker Email', 'Check-in Date', 'Check-out Date', 'Property', 'Owner Email', 'Room Type',  
                    'Unit(s)', 'Total Guest', 'Room / Per Night / Price/每晚/價格', 'Day(s)', 'Total Room Rate', 'Room / Cleaning Fee/清潔費', 'Room / Service Fee/服務費', 'Total Amount',
                    'ipay88 MDR', 'Payment Method_x', 'Status', 'TA Commission - 10% (RM)', 'Host Commission - 12% (RM)', 'Total Amount After Commission (RM)', 'Total Amount Pay to Host (RM)'] 

    df_zuba = df_zuba[report_order]

    #Change column name 
    df_zuba.rename(columns={'Room / Per Night / Price/每晚/價格': 'Room / Per Night / Price/每晚/價格 (RM)'}, inplace=True)
    df_zuba.rename(columns={'Room / Cleaning Fee/清潔費': 'Room / Cleaning Fee/清潔費 (RM)'}, inplace=True)
    df_zuba.rename(columns={'Room / Service Fee/服務費': 'Room / Service Fee/服務費 (RM)'}, inplace=True)
    df_zuba.rename(columns={'Room / Tax/稅金': 'Room / Tax/稅金 (RM)'}, inplace=True)
    df_zuba.rename(columns={'Total Room Rate': 'Total Room Rate (RM)'}, inplace=True)
    
    # Sort the DataFrame by the 'Room Type' column
    df_zuba = df_zuba.sort_values(by='Room Type', ascending=True)

    # Group by "Owner Email" and calculate the sum 
    total_host = df_zuba.groupby('Owner Email')['Total Amount Pay to Host (RM)'].sum().reset_index()
    total_com = df_zuba.groupby('Owner Email')['Total Amount After Commission (RM)'].sum().reset_index()

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
                'Total Amount After Commission (RM)': [total_com.loc[total_com['Owner Email'] == owner_email, 'Total Amount After Commission (RM)'].values[0]],
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

    return df_zuba_result