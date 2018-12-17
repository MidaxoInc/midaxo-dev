
values = df['COMPANIES']
phases = df['MEASURE']
colors = ['rgb(32,155,160)',
          'rgb(253,93,124)',
          'rgb(28,119,139)',
          'rgb(182,231,235)']
n_phase = len(phases)
plot_width = 800

# height of a section and difference between sections 
section_h = 100
section_d = 10

# multiplication factor to calculate the width of other sections
unit_width = plot_width / max(values)

# width of each funnel section relative to the plot width
phase_w = [int(value * unit_width) for value in values]

# plot height based on the number of sections and the gap in between them
height = section_h * n_phase + section_d * (n_phase - 1)

# list containing all the plot shapes
shapes = []

# list containing the Y-axis location for each section's name and value text
label_y = []

for i in range(n_phase):
        if (i == n_phase-1):
                points = [phase_w[i] / 2, height, phase_w[i] / 2, height - section_h]
        else:
                points = [phase_w[i] / 2, height, phase_w[i+1] / 2, height - section_h]

        path = 'M {0} {1} L {2} {3} L -{2} {3} L -{0} {1} Z'.format(*points)

        shape = {
                'type': 'path',
                'path': path,
                'fillcolor': colors[i],
                'line': {
                    'width': 1,
                    'color': colors[i]
                }
        }
        shapes.append(shape)
        
        # Y-axis location for this section's details (text)
        label_y.append(height - (section_h / 2))

        height = height - (section_h + section_d)

# For phase names
label_trace = go.Scatter(
    x=[-300]*n_phase,
    y=label_y,
    mode='text',
    text=phases,
    textfont=dict(
        color='#404040',
        size=15
    )
)
 
# For phase values
value_trace = go.Scatter(
    x=[300]*n_phase,
    y=label_y,
    mode='text',
    text=values,
    textfont=dict(
        color='#404040',
        size=15
    )
)

data = [label_trace, value_trace]
 
layout = go.Layout(
    shapes=shapes,
    height=450,
    width=700,
    showlegend=False,
    paper_bgcolor='transparent',
    plot_bgcolor='transparent',
    xaxis=dict(
        showticklabels=False,
        zeroline=False,
        showgrid=False,
    ),
    yaxis=dict(
        showticklabels=False,
        zeroline=False,
        showgrid=False,
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