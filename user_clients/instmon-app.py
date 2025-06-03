import pandas as pd
import copy
from datetime import date

from bokeh.layouts import column, row
from bokeh.models import Button, TextInput, Select, HoverTool, \
    ColumnDataSource, CategoricalColorMapper, DatePicker
from bokeh.plotting import figure, curdoc

# create a plot and style its properties
# create a new plot with a title and axis labels
p = figure(title="GMOS Bias monitoring",
           x_axis_label='Datalabel',
           y_axis_label='Statistic',
           height=512,
           width=1024,
           )

# Color mapper on qastate
mapper = CategoricalColorMapper(palette=["red", "orange", "blue", "green"],
                                factors=["Fail", "Usable", "Undefined", "Pass"])

# Add the scatter and line plots, dummy data
source = ColumnDataSource(data=dict(x=[], y=[], qastate=[]))
s = p.scatter(x='x', y='y', source=source, # size=6,
              fill_color={"field": "qastate", "transform": mapper},
              line_color={"field": "qastate", "transform": mapper},
              )
l = p.line(x='x', y='y', source=source)

# Initialize empty data values
df = []
fdf = []
binnings = []
gains = []
read_speeds = []
rois = []
plots = []

# Add the widgets
datatext = TextInput(prefix="Data Source: ", sizing_mode="stretch_width",
                     value="/Users/phirst/newnewdev/bias-analysis/TEST.tsv")
statustext = TextInput(prefix="Status:", sizing_mode="stretch_width")
button = Button(label="Load")
binning_select = Select(title="Binning", options=binnings, value='Any')
gain_select = Select(title="Gain", options=gains, value='Any')
read_speed_select = Select(title="Read Speed", options=read_speeds, value='Any')
roi_select = Select(title="ROI", options=rois, value='Any')
startdate_picker = DatePicker(title="Start Date", min_date=date(2000, 1, 1),
                              max_date=date(2000, 1, 1))
enddate_picker = DatePicker(title="End Date", min_date=date(2000, 1, 1),
                            max_date=date(2000, 1, 1))


plot_select = Select(title="Plot", options=plots, value='Any')

# Define callback functions

def load_data():
    global fdf

    print(f"loading {datatext.value} into fdf")
    fdf = pd.read_csv(datatext.value, sep='\t', header=0)
    fdf['ut_datetime'] = pd.to_datetime(fdf['ut_datetime'], format='ISO8601')
    fdf.sort_values(by=['ut_datetime', 'adid'], inplace=True)
    print(f"In load_data {len(fdf)=}")

    binnings = ['Any'] + list(fdf['binning'].unique())
    gains = ['Any'] + list(fdf['gain'].unique())
    read_speeds = ['Any'] + list(fdf['read_speed'].unique())
    rois = ['Any'] + list(fdf['roi'].unique())

    binning_select.options = binnings
    gain_select.options = gains
    read_speed_select.options = read_speeds
    roi_select.options = rois
    plot_select.options = list(fdf.columns)
    mindate = fdf['ut_datetime'].min().date()
    maxdate = fdf['ut_datetime'].max().date()
    startdate_picker.min_date = mindate
    startdate_picker.max_date = maxdate
    startdate_picker.value = mindate
    enddate_picker.min_date = mindate
    enddate_picker.max_date = maxdate
    enddate_picker.value = maxdate

    print(f"Loaded {len(fdf)} rows")
    statustext.value = f"Loaded {len(fdf)} rows."

    # Don't add these callbacks until after we have set the default values
    startdate_picker.on_change('value', select_callback)
    enddate_picker.on_change('value', select_callback)

def filter_data():
    global df
    global s
    global xs
    global ys
    global cds

    df = copy.copy(fdf)
    print(f"Starting filter_data, {len(fdf)=} {len(df)=}")
    if binning_select.value != 'Any':
        df = df[df['binning'] == binning_select.value]
    if gain_select.value != 'Any':
        df = df[df['gain'] == gain_select.value]
    if read_speed_select.value != 'Any':
        df = df[df['read_speed'] == read_speed_select.value]
    if roi_select.value != 'Any':
        df = df[df['roi'] == roi_select.value]
    print(f"{type(startdate_picker.value)=}")
    mindate = fdf['ut_datetime'].min().date()
    maxdate = fdf['ut_datetime'].max().date()
    startdate_date = pd.to_datetime(startdate_picker.value, format='ISO8601')
    enddate_date = pd.to_datetime(enddate_picker.value, format='ISO8601')
    if startdate_date != mindate:
        df = df[df['ut_datetime'] >= startdate_date]
    if enddate_date != maxdate:
        df = df[df['ut_datetime'] <= enddate_date]

    print(f"Post-filter {len(df)=}")

    print("adding dummy split rows")
    # Add "-1" adid rows for each datalabel with values NaN to cause line plot
    # to split data labels
    # Make a dataframe with all the new rows in and concat it to df.
    # Make the new columns as lists
    newdls = []
    newadids = []
    newutdts = []
    dls = df['data_label'].unique()
    for dl in dls:
        a1 = df[(df['data_label']==dl) & (df['adid']==1)]
        newdls.append(a1.iloc[0]['data_label'])
        newutdts.append(a1.iloc[0]['ut_datetime'])
        newadids.append(-1)
    newdf = pd.DataFrame({'data_label': newdls, 'ut_datetime': newutdts,
                          'adid': newadids})
    df = pd.concat([df, newdf], ignore_index=True)
    df.sort_values(by=['ut_datetime', 'adid'], inplace=True)
    print("added dummy split rows")

    df['row_number'] = df.reset_index().index
    df = df.set_index('row_number')
    print(f"Filtered down to {len(df)} rows.")

    statustext.value = f"Filtered down to {len(df)} rows."
    s.data_source = ColumnDataSource(df)
    l.data_source = ColumnDataSource(df)
    p.xaxis.major_label_overrides = dict(df['data_label'])
    p.xaxis.major_label_orientation = "vertical"
    # Hover tooltips
    hover = HoverTool(tooltips=[
        ("DL", "@data_label"),
        ("ext", "@adid"),
        ("qastate", "@qastate"),
    ])
    p.add_tools(hover)

def select_callback(attr, old, new):
    filter_data()

def plot_callback(attr, old, new):
    global s
    global l
    s.glyph.x = 'row_number'
    s.glyph.y = new
    l.glyph.x = 'row_number'
    l.glyph.y = new
    statustext.value = f"Plotted {s.glyph.y}"

button.on_event('button_click', load_data)
binning_select.on_change('value', select_callback)
gain_select.on_change('value', select_callback)
roi_select.on_change('value', select_callback)
read_speed_select.on_change('value', select_callback)

plot_select.on_change('value', plot_callback)

# put the button and plot in a layout and add to the document
loadrow = row(datatext, button, sizing_mode="stretch_width")
loadblock = column(statustext, loadrow, sizing_mode="stretch_width")
selectrow = row(binning_select, gain_select, read_speed_select, roi_select,
        startdate_picker, enddate_picker, plot_select)

curdoc().add_root(column(loadblock, selectrow, p))
