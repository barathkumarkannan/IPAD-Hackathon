import pygwalker as pyg
import pandas as pd
import streamlit.components.v1 as components
import streamlit as st

# Adjust the width of the Streamlit page
def transformation_graph(df):
    st.set_page_config(
    page_title="Transformation Data",
    layout="wide"
)

# Add Title
    st.title("Transformation Data")

# Import your data
#     df = pd.read_csv("/Users/sivasankarann/Downloads/rankings_2011.csv",index_col = 0)
    # print(df['report'][1])
    # df1 = pd.read_csv(df['report'][1])

# Generate the HTML using Pygwalker
    pyg_html = pyg.walk(df.cache(), return_html=True)

# Embed the HTML into the Streamlit app
    components.html(pyg_html, height=1000, scrolling=True)