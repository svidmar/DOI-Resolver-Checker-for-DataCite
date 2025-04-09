import requests
import csv
import time
import streamlit as st
import pandas as pd
from tqdm import tqdm
from io import StringIO
import hashlib
import concurrent.futures

# === STREAMLIT INTERFACE ===
st.set_page_config(page_title="DOI Resolver Checker", layout="centered")
st.title("DOI Resolver Checker for DataCite")

st.markdown("""
This app connects to the DataCite API to fetch all DOIs for a given prefix and checks whether each DOI resolves correctly.

### ℹ️ How to Use the Tool

1. Enter your **DataCite Fabrica username and password**.  
2. Provide your **DOI prefix** (e.g., `10.12345`).  
3. Click **“Check DOI Resolution”** to fetch all DOIs registered with that prefix  
4. You’ll see a summary chart with the columns ("DOI", "Registered URL", "Resolves (Y/N)", "HTTP Status Code") and can **download a CSV report** of the results.

---

### 🔒 Credentials

Your credentials are used **only** to authenticate securely with the DataCite API via HTTPS.  
They are **not stored**, **not shared**, and are discarded after the session ends.

---

**Creator:**  
Søren Vidmar  
🔗 [ORCID](https://orcid.org/0000-0003-3055-6053)  
🏫 Aalborg University  
📧 Email: [sv@aub.aau.dk](mailto:sv@aub.aau.dk)  
🏗 GitHub: [github.com/svidmar](https://github.com/svidmar)
""")

with st.expander("🔐 Enter your DataCite credentials"):
    username = st.text_input("DataCite Username", type="default")
    password = st.text_input("DataCite Password", type="password")
prefix = st.text_input("DOI Prefix (e.g., 10.12345)")

start_check = st.button("Check DOI Resolution")

RETRY_DELAY = 2.0
MAX_RETRIES = 3
PER_PAGE = 1000
MAX_WORKERS = 10  # Parallel requests

@st.cache_data(show_spinner=False)
def fetch_all_dois(username, password, prefix):
    page = 1
    dois = []
    while True:
        url = f"https://api.datacite.org/dois?query=prefix:{prefix}&page[size]={PER_PAGE}&page[number]={page}"
        response = requests.get(url, auth=(username, password))
        if response.status_code != 200:
            st.error(f"Failed to fetch DOIs: {response.status_code} - {response.text}")
            return []
        data = response.json()
        items = data.get("data", [])
        if not items:
            break
        for item in items:
            doi = item["id"]
            registered_url = item["attributes"].get("url", "")
            dois.append((doi, registered_url))
        page += 1
        time.sleep(1)
    return dois

def check_doi_resolves(doi):
    url = f"https://doi.org/{doi}"
    retries = 0
    while retries <= MAX_RETRIES:
        try:
            response = requests.head(url, allow_redirects=True, timeout=10)
            status_code = response.status_code
            resolves = "Yes" if status_code in [200, 301, 302] else "No"
            return doi, resolves, status_code
        except requests.RequestException:
            retries += 1
            time.sleep(RETRY_DELAY)
    return doi, "No", "Timeout/Error"

def generate_csv(results):
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["DOI", "Registered URL", "Resolves", "HTTP Status Code"])
    writer.writerows(results)
    return output.getvalue()

def hash_credentials(username, password):
    combined = f"{username}:{password}"
    return hashlib.sha256(combined.encode()).hexdigest()

if start_check and username and password and prefix:
    credentials_hash = hash_credentials(username, password)
    st.info(f"Credentials hash: {credentials_hash[:8]}... (used internally, not stored)")

    with st.spinner("Fetching DOIs from DataCite..."):
        dois = fetch_all_dois(username, password, prefix)

    if dois:
        st.info("Checking DOI resolution using parallel requests...")
        results = []
        progress_text = st.empty()
        progress_bar = st.progress(0)

        resolved_map = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(check_doi_resolves, doi): (doi, reg_url) for doi, reg_url in dois}
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                doi, resolves, status_code = future.result()
                reg_url = dict(futures.values())[doi]
                results.append((doi, reg_url, resolves, status_code))
                progress_bar.progress((i + 1) / len(futures))
                progress_text.text(f"Checked {i + 1} of {len(futures)}")

        st.success("DOI checks complete!")

        # Convert to DataFrame
        df = pd.DataFrame(results, columns=["DOI", "Registered URL", "Resolves", "HTTP Status Code"])
        st.dataframe(df)

        # Summary chart
        summary = df["Resolves"].value_counts().reset_index()
        summary.columns = ["Resolves", "Count"]
        st.bar_chart(summary.set_index("Resolves"))

        # CSV export
        csv_data = generate_csv(results)
        st.download_button("📥 Download CSV Report", csv_data, file_name="doi_status_report.csv", mime="text/csv")
else:
    st.info("Enter your credentials and prefix to begin.")
