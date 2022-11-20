import numpy as np
import plotly.graph_objects as go
import streamlit as st
from annotated_text import annotated_text

from scipy.stats import linregress
from function import *

coin_list = ["Bitcoin", "Ethereum",\
            "Ripple", "Solana", \
            "Cardano", "Binance Coin",\
            "Tether", "DogeCoin"]

with st.sidebar:
    st.title("Crypto Analyzer")
    #st.image("https://cdn-icons-png.flaticon.com/512/1213/1213797.png", width=200)
    choice = st.selectbox("Select Your Coin", coin_list)

df = getCrypto(choice)
twt = getSentiment(choice)

x = np.arange(1,len(df)+1)
stock_res = linregress(x, df['prices'])
cap = round(df['market_caps'].mean(), 2)
vol = round(df['total_volumes'].mean(), 2)
sentiment_value = twt['score'].mean()

if (stock_res[0] > 0) & (sentiment_value > 0):
    value1 = "GOOD"
    color = '#ADD8E6'
if (stock_res[0] <= 0) & (sentiment_value > 0):
    value1 = "PROSPECT"
    color = '#56A5EC'
if (stock_res[0] >= 0) & (sentiment_value < 0):
    value1 = "CAUTIOUS"
    color = '#F88017'
else: 
    value1 = "BAD"
    color = '#FF2400'

if cap > 2*vol:
    value2 = "SAFE"
    color2 = "#5EFB6E"
elif (cap > vol) & (cap < 2*vol):
    value2 = "MEDIUM"
    color2 = "#FFFF33"
else:
    value2 = "RISKY"
    color2 = "#FF2400"

annotated_text("Public sentiments are ", (value1,"",color), 
                "  |  Market Value is ", (value2,"",color2))

# set up plotly figure
fig = go.Figure(layout_title_text=choice+' Price (last 7 days)')

# add line figure 1
fig.add_trace(go.Scatter(
    name='Price',
    x=df.index,
    y=df['prices'],
    hovertext=df.index,
    hoverinfo='x'
))

fig.update_yaxes(title_text='USD')
fig.update_layout({
    'plot_bgcolor': 'rgba(0,0,0,0)',
    'paper_bgcolor': 'rgba(0,0,0,0)'})

st.plotly_chart(fig, use_container_width=True)

# set up plotly figure
fig2 = go.Figure(layout_title_text=choice+' Market Cap & Total Volume (last 7 days)')

# add line / trace 1 to figure
fig2.add_trace(go.Scatter(
    name='Market Cap',
    x=df.index,
    y=df['market_caps'],
    hovertext=df.index,
    hoverinfo='x',
    fill='tozeroy'
))

# add line / trace 2 to figure
fig2.add_trace(go.Scatter(
    name='Total Volumes',
    x=df.index,
    y=df['total_volumes'],
    hovertext=df.index,
    hoverinfo='x',
    fill='tozeroy'
))

fig2.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1.02,
    xanchor="right",
    x=1
))

fig2.update_yaxes(title_text='Million USD')
fig2.update_layout({
    'plot_bgcolor': 'rgba(0,0,0,0)',
    'paper_bgcolor': 'rgba(0,0,0,0)'})

st.plotly_chart(fig2, use_container_width=True)



