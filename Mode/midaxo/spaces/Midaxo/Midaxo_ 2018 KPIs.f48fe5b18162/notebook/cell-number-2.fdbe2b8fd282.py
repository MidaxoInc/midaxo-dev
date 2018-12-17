from plotly.offline import download_plotlyjs, init_notebook_mode, iplot
import plotly as py
import plotly.graph_objs as go
py.offline.init_notebook_mode()
import plotly.graph_objs as go
import pandas as pd

df = datasets["a717b7c29eaa"]
x_data = df['METRIC']
y_data = df['VALUE']
text = df['VALUE']

trace0 = go.Bar(
  x=x_data,
  y=[0,df.VALUE[0],sum(df.VALUE[0:2]),sum(df.VALUE[0:3]),sum(df.VALUE[0:4]),sum(df.VALUE[0:5]),sum(df.VALUE[0:6]),sum(df.VALUE[0:7]),0],
   marker=dict(
        color='rgba(0,0,0,0)'),
    hoverinfo = 'none',
  )
trace1 = go.Bar(
  x=x_data,
  y=[df.VALUE[0],df.VALUE[1],df.VALUE[2],df.VALUE[3],df.VALUE[4],df.VALUE[5],df.VALUE[6],df.VALUE[7],df.VALUE[8]],
  marker = dict(
    color = ['#4BB876' if val>=0 else '#E55766' for val in y_data]),
  )  
  
data = [trace0, trace1]
layout = go.Layout(
  barmode='stack',
  paper_bgcolor='#fff',
  plot_bgcolor='#fff',
  showlegend=False,
  yaxis = dict(
    range=[0,9000000],
    tickfont=dict(
            family='Arial, sans-serif',
            size=11,
            color='grey'
        ),
    ),
  xaxis = dict(
    tickfont = dict(
        family = 'Arial, sans-serif',
        size = 11,
        color = 'grey'
        ),
    ),
  margin=go.Margin(
        l=40,
        r=40,
        b=40,
        t=40,
        pad=.1
        ),
  bargap = 0.1,
  )
  

fig = go.Figure(data=data, layout=layout)
iplot(fig)