import streamlit as st
import pandas as pd
import requests

UPLOAD_URL = "http://127.0.0.1:8000/upload-csv/"
FETCH_URL  = "http://127.0.0.1:8000/get-uploaded-data/"

st.title("CSV → FastAPI → Pandas → JSON → MySQL → Streamlit Viewer")

uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

if uploaded_file:
    if st.button("Upload CSV → FastAPI → Save to MySQL"):
        with st.spinner("Uploading & Processing..."):
            files = {"file": uploaded_file}
            response = requests.post(UPLOAD_URL, files=files)

            if response.status_code == 200:
                result = response.json()

                if result["status"] == "success":
                    st.success(result["message"])
                else:
                    st.error(result["message"])
            else:
                st.error("Backend not reachable")

# --------------------------------------------------
# NEW BUTTON: FETCH SAVED DATA FROM MYSQL
# --------------------------------------------------
if st.button("📥 Fetch Saved Data from MySQL"):
    with st.spinner("Loading data from database..."):
        response = requests.get(FETCH_URL)

        if response.status_code == 200:
            result = response.json()

            if result["status"] == "success":
                records = result["records"]

                st.subheader("📌 Latest Uploaded Records")

                for row in records:
                    st.markdown(f"### File: {row['filename']} (ID: {row['id']})")
                    df = pd.DataFrame(row["data"])
                    st.dataframe(df, use_container_width=True)
                    st.text(f"Uploaded At: {row['uploaded_at']}")
                    st.markdown("---")
            else:
                st.error(result["message"])
        else:
            st.error("Could not connect to FastAPI backend.")
