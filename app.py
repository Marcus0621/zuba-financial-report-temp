import streamlit as st
import pandas as pd
import io

# Title for the Streamlit app
st.title("Excel File Processor")

# Upload Excel file
uploaded_file = st.file_uploader("Upload an Excel file", type=["xlsx"])

# Check if the file is uploaded
if uploaded_file is not None:
    # Read the uploaded Excel file into a DataFrame
    df = pd.read_excel(uploaded_file)

    # Drop the specified columns
    columns_to_remove = ['Payment Category', 'Card type', 'Remark', 'TerminalID',
                         'Forex Rate', 'Payment Notice (VT Only)', 'Original Amount',
                         'Discount', 'GST', 'Comm (MYR)', 'Original Currency']
    df = df.drop(columns=columns_to_remove)

    # Calculate "Zuba Total" and "Landlord Total"
    df['Zuba Total'] = df['Total (MYR)'] * 0.12
    df['Landlord Total'] = df['Total (MYR)'] * 0.88
    df['Merchant RefNo'] = df['Merchant RefNo'].astype(str)  # Convert to string

    # Sort the DataFrame by the 'ProdDesc' column
    df = df.sort_values(by='ProdDesc', ascending=True)

    # Group by 'ProdDesc' and calculate totals
    totals = (
        df.groupby('ProdDesc')[['Total (MYR)', 'Zuba Total', 'Landlord Total']].sum().reset_index()
    )

    # Add totals under each group in the DataFrame
    output = []
    for prod_desc, group in df.groupby('ProdDesc'):
        output.append(group)  # Add original group
        # Create a totals row with NaN for non-relevant columns
        total_row = pd.DataFrame(
            {
                'ProdDesc': ['Total'],
                'Total (MYR)': [totals.loc[totals['ProdDesc'] == prod_desc, 'Total (MYR)'].values[0]],
                'Zuba Total': [totals.loc[totals['ProdDesc'] == prod_desc, 'Zuba Total'].values[0]],
                'Landlord Total': [totals.loc[totals['ProdDesc'] == prod_desc, 'Landlord Total'].values[0]],
            }
        )
        output.append(total_row)  # Add totals row

    # Combine everything into a single DataFrame
    result_df = pd.concat(output, ignore_index=True)

    # Show the processed data (without download)
    st.write(result_df)
