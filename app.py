
import streamlit as st
import pandas as pd
from db import execute, df_insert

st.set_page_config(page_title="Amazon FIFO Inline", layout="wide")
st.title("Amazon FIFO Inline Test")

# Inline inbound entry demo
batch_id = st.text_input("Batch ID")
items_df = st.data_editor(pd.DataFrame(columns=["internal_sku","category","qty_in","fob_unit","cbm_per_unit"]),
                          num_rows="dynamic", use_container_width=True)
if st.button("Commit Items"):
    if not batch_id:
        st.error("Batch ID required")
    else:
        if items_df.empty:
            st.warning("No rows")
        else:
            items_df.insert(0, "batch_id", batch_id)
            df_insert(items_df, "inbound_items_flat", truncate_first=True)
            st.success("Inserted items for batch " + batch_id)
