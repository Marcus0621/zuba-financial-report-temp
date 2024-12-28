# app.py

import streamlit as st
import pandas as pd
import io
from data_processing import process_data  # Import the function from data_processing.py

# Title for the Streamlit app
st.title("Temporary Zuba Financial Report v22")

# Upload Excel file
uploaded_file = st.file_uploader("Upload Zuba Transaction Record Excel File:", type=["xlsx"])

# Check if the file is uploaded
if uploaded_file is not None:

    st.success("File uploaded successfully!")

    # Read the uploaded Excel file into a DataFrame
    df_zuba = pd.read_excel(uploaded_file)

    # Process the data using the function from data_processing.py
    result_df = process_data(df_zuba)

    # Show the processed data (optional, to let the user review the output)
    st.write(result_df)

    # Convert the DataFrame to an Excel file in memory (avoid writing to file system)
    result_file = io.BytesIO()
    with pd.ExcelWriter(result_file, engine="xlsxwriter") as writer:
        result_df.to_excel(writer, index=False, sheet_name="Processed Data")
    result_file.seek(0)  # Reset pointer to the beginning

# Provide download button for the processed file
    download_button = st.download_button(
        label="Download Report",
        data=result_file,
        file_name="Financial_Report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True  # Make the button span the width of the container
    )

