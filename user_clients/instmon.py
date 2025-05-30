from bokeh.plotting import figure, show
from bokeh.models import (ColumnDataSource, DatetimeTickFormatter,
                          CategoricalColorMapper, HoverTool)
import pandas as pd
import copy

fdf = pd.read_csv("/Users/phirst/newnewdev/bias-analysis/TEST.tsv", sep='\t', header=0)
fdf['ut_datetime'] = pd.to_datetime(fdf['ut_datetime'], format='ISO8601')
fdf.sort_values(by=['ut_datetime', 'adid'], inplace=True)

print(fdf.head())

df = copy.copy(fdf)
df = df[df['binning']=="1x1"]
df = df[df['read_speed']=='slow']
df = df[df['gain']=='low']
df = df[df['roi']=='Full Frame']
#df = df[df['label']=="'BI13-20-4k-1, 1':[1537:2048,1:4224]"]

df['row_number'] = df.reset_index().index
df = df.set_index('row_number')
print(df.head(50))

print(f"{len(df)=}")

# create a new plot with a title and axis labels
p = figure(title="GMOS Bias monitoring",
           x_axis_label='Datalabel',
           y_axis_label='Statistic',
           #x_axis_type="datetime",
           height=512,
           width=1024,
           sizing_mode="stretch_width",
           )

# Format the x-axis tips
#p.xaxis[0].formatter = DatetimeTickFormatter(days="%F")

# Hover tooltips
hover = HoverTool(tooltips=[
    ("DL", "@data_label"),
    ("ext", "@adid"),
    ("qastate", "@qastate"),
    ])
p.add_tools(hover)

# Color mapper on qastate
mapper = CategoricalColorMapper(palette=["red", "orange", "blue", "green"],
                                factors=["Fail", "Usable", "Undefined", "Pass"])

# Add the scatter plot
s = p.scatter('row_number', 'BICOMED', source=df,
          fill_color={"field":"qastate", "transform":mapper},
          line_color={"field": "qastate", "transform":mapper},
          size=8,
          )

# Add line plots for each data label...
for dl in df['data_label'].unique():
    p.line('row_number', 'BICOMED', source=df[df['data_label']==dl])

p.xaxis.major_label_overrides = dict(df['data_label'])

# show the results
show(p)