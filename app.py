# app.py

import streamlit as st
import pandas as pd
import io
from data_processing import process_data_zuba  # Import zuba data processing function
from data_processing import process_data_ipay # Import ipay data processing function
from data_processing import merge_dataset # Import merge dataset function

# Title for the Streamlit app
st.title("Temporary Zuba Financial Report")

#Upload Excel file from ipay Report
uploaded_file_ipay = st.file_uploader("**Upload ipay88 Transaction Record Xlsx:**", type=["xlsx"])

# Upload Excel file for Zuba Transaction 
uploaded_file_zuba = st.file_uploader("**Upload Zuba Transaction Record Xlsx:**", type=["xlsx"])

# Check if the file is uploaded
if (uploaded_file_zuba is not None) & (uploaded_file_ipay is not None):

    # try:
        st.success("File uploaded successfully!")

        # Read ipay88 uploaded Excel File into DataFrame
        df_ipay = pd.read_excel(uploaded_file_ipay)

        # Read Zuba uploaded Excel file into a DataFrame
        df_zuba = pd.read_excel(uploaded_file_zuba) 
        
        # Process Zuba data using the function from data_processing.py
        zuba_result_df = process_data_zuba(df_zuba)

        # Process ipay data using the function from data_processing.py
        ipay_result_df = process_data_ipay(df_ipay)

        # Call function to further process and merge dataset
        final_report = merge_dataset(zuba_result_df, ipay_result_df)

        # Show the processed data (optional, to let the user review the output)
        st.write("\n")
        st.write("\n")
        st.write("\n")
        st.write("*Processed ipay88 Transaction Report:*")
        st.write(ipay_result_df)
        st.write("*Processed Zuba Transaction Report:*")
        st.write(zuba_result_df)
        st.write("\n")
        st.write("\n")
        st.write("**Final Financial Report:**")
        st.write(final_report)

        # Convert the DataFrame to an Excel file in memory (avoid writing to file system)
        result_file = io.BytesIO()
        with pd.ExcelWriter(result_file, engine="xlsxwriter") as writer:
            final_report.to_excel(writer, index=False, sheet_name="Processed Data")
        result_file.seek(0)  # Reset pointer to the beginning

    # Provide download button for the processed file
        download_button = st.download_button(
            label="Download Report",
            data=result_file,
            file_name="Financial_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True  # Make the button span the width of the container
        )

    # except Exception as e:
    #     st.error("Wrong Report Format")
