# data_processing.py

import pandas as pd

def process_data(df_zuba):    
    # Clean column names by removing leading/trailing spaces
    df_zuba.columns = df_zuba.columns.str.strip()

    # Column formatting for easier calculation 
    df_zuba["Room / Per Night / Price/每晚/價格"] = pd.to_numeric(df_zuba["Room / Per Night / Price/每晚/價格"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Room / Cleaning Fee/清潔費"] = pd.to_numeric(df_zuba["Room / Cleaning Fee/清潔費"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Room / Service Fee/服務費"] = pd.to_numeric(df_zuba["Room / Service Fee/服務費"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Room / Tax/稅金"] = pd.to_numeric(df_zuba["Room / Tax/稅金"].str.replace('RM', '').str.strip(), errors='coerce')
    df_zuba["Day(s)"] = pd.to_numeric(df_zuba["Day(s)"].str.replace('天', '').str.strip(), errors='coerce')

    # Calculate "Total Room Rate" and add it as a new column
    df_zuba["Total Room Rate"] = df_zuba["Room / Per Night / Price/每晚/價格"] * df_zuba["Day(s)"]
    df_zuba['Host Commision 12%'] = df_zuba["Total Room Rate"] * 12/100
    df_zuba['Total Amount After Commission'] = df_zuba["Total Room Rate"] - df_zuba['Host Commision 12%']
    df_zuba['Total Amount Pay to Host'] = df_zuba["Room / Cleaning Fee/清潔費"] + df_zuba["Room / Service Fee/服務費"] + df_zuba["Room / Tax/稅金"] + df_zuba['Total Amount After Commission']

    # Sort the DataFrame by the 'Room Type' column (ensure the column name matches exactly)
    df_zuba = df_zuba.sort_values(by='Room Type', ascending=True)

    # Group by "Owner Email" and calculate the sum for "Total Amount Pay to Host"
    totals = df_zuba.groupby('Owner Email')['Total Amount Pay to Host'].sum().reset_index()

    # Prepare a list to store the output DataFrame with totals rows
    output = []

    # Group by "Owner Email" and add totals row under each group
    for owner_email, group in df_zuba.groupby('Owner Email'):
        output.append(group)  # Add the original group data
        # Create a totals row with NaN for non-relevant columns
        total_row = pd.DataFrame(
            {
                'Owner Email': ['Total'],
                'Total Amount Pay to Host': [totals.loc[totals['Owner Email'] == owner_email, 'Total Amount Pay to Host'].values[0]],
            }
        )

        #only to create empty row below
        # empty_row = pd.DataFrame(
        #     {
        #         'Owner Email': ['']
        #     }
        # )

        # Add rows 
        output.append(total_row)  
        # output.append(empty_row)

    # Combine everything into a single DataFrame
    result_df = pd.concat(output, ignore_index=True)

    return result_df
