import pandas as pd
import copy
from datetime import date

from bokeh.layouts import column, row
from bokeh.models import Button, TextInput, Select, HoverTool, \
    ColumnDataSource, CategoricalColorMapper, DatePicker, Div, \
    BasicTickFormatter
from bokeh.plotting import figure, curdoc

# create a plot with a title and axis labels
p = figure(title="Instrument monitoring",
           x_axis_label='Data Label',
           y_axis_label='Statistic',
           height=512,
           width=1024,
           )
p.yaxis.formatter = BasicTickFormatter(use_scientific=False)
p.xaxis.ticker.desired_num_ticks=10

# Color mapper for qastate
mapper = CategoricalColorMapper(palette=["red", "orange", "blue", "green"],
                                factors=["Fail", "Usable", "Undefined", "Pass"])

# Add the scatter plot, with dummy data but configure the color mapper
source = ColumnDataSource(data=dict(x=[], y=[], qastate=[]))
s = p.scatter(x='x', y='y', source=source, # size=6,
              fill_color={"field": "qastate", "transform": mapper},
              line_color={"field": "qastate", "transform": mapper})

# Add the Hover tool, we configure it in filter_data
p.add_tools(HoverTool())

# Initialize empty data values
df = []
fdf = []
plots = []

# Add the widgets
datatext = TextInput(prefix="Data Source: ", sizing_mode="stretch_width")
statustext = TextInput(prefix="Status:", sizing_mode="stretch_width")
url_prefix_text = TextInput(prefix="URL builder:", sizing_mode="fixed",
                            width=340,
                            value="http://arctest.gemini.edu/monitoring")
url_report_select = Select(options=["checkBias", "checkFlat"])
url_inst_select = Select(options=["GMOS-N", "GMOS-S"])
url_binning_select = Select(options=[None, "1x1", "2x2"])
url_roi_select = Select(options=[None, "fullframe", "centralspectrum"])
url_startdate_picker = DatePicker()
url_enddate_picker = DatePicker()
url_build_button = Button(label="Build URL")
load_button = Button(label="Load")
group_select = Select(title="Group by", options=['None', 'max'], value='None')
plot_select = Select(title="Plot", options=plots, value='Any')
plot_default_button = Button(label="Default", align='end')


# Define callback functions

def build_url():
    select_assist = None
    if url_report_select.value == 'checkBias':
        select_assist = 'BIAS/DayCal/Raw'
    elif url_report_select.value == 'checkFlat':
        select_assist = 'OBJECT/DayCal/Raw/object=Twilight'
    all_items = [url_prefix_text.value, url_report_select.value, select_assist,
                 url_inst_select.value, url_binning_select.value,
                 url_roi_select.value]
    if url_startdate_picker.value and url_enddate_picker.value:
        all_items.append(f"{url_startdate_picker.value.replace('-', '')}-"
                         f"{url_enddate_picker.value.replace('-', '')}")
    elif url_startdate_picker.value:
        all_items.append(url_startdate_picker.value.replace('-', ''))
    items = [i for i in all_items if i]
    datatext.value = '/'.join(items)

def load_data():
    global fdf

    print(f"loading {datatext.value} into fdf")
    fdf = pd.read_csv(datatext.value, sep='\t', header=0)
    fdf['ut_datetime'] = pd.to_datetime(fdf['ut_datetime'], format='ISO8601')
    fdf.sort_values(by=['ut_datetime', 'adid'], inplace=True)

    plot_select.options = list(fdf.columns)

    print(f"Loaded {len(fdf)} rows")
    statustext.value = f"Loaded {len(fdf)} rows."

    filter_data()

def filter_data():
    global df
    global s
    global cds
    global h

    df = copy.copy(fdf)
    print(f"Starting filter_data, {len(df)=}")

    if group_select.value == 'max':
        df = df.groupby('data_label', as_index=False).max()
    print(f"Post-group {len(df)=}")

    df['row_number'] = df.reset_index().index
    df = df.set_index('row_number')
    print(f"Filtered down to {len(df)} rows.")
    statustext.value = f"Filtered down to {len(df)} rows."

    s.data_source = ColumnDataSource(df)
    labels = dict(df['data_label'])
    for i in labels.keys():
        dlsplit = labels[i].split('-')
        if len(dlsplit) == 4:
            labels[i] = dlsplit[1]
    p.xaxis.major_label_overrides = labels
    p.xaxis.major_label_orientation = "vertical"

    # This slightly bizzare idiom seems to be necessary to get the hovertool to
    # notice that the data_source changed...
    p.hover.tooltips = []
    p.hover.tooltips=[("DL", "@data_label"), ("ext", "@adid"),
                ("qastate", "@qastate")]


def select_callback(attr, old, new):
    filter_data()

def plot_callback(attr, old, new):
    s.glyph.x = 'row_number'
    s.glyph.y = new
    p.yaxis.axis_label = new

    statustext.value = f"Plotted {s.glyph.y}"

def plot_default_callback():
    global plot_select
    global group_select
    global url_report_select
    if url_report_select.value == 'checkBias':
        group_select.value = 'max'
        plot_select.value = 'OSCOMED'
    elif url_report_select.value == 'checkFlat':
        group_select.value = 'max'
        plot_select.value = 'FLATMED'


load_button.on_event('button_click', load_data)
url_build_button.on_event('button_click', build_url)
plot_default_button.on_event('button_click', plot_default_callback)

group_select.on_change('value', select_callback)
plot_select.on_change('value', plot_callback)

# put the button and plot in a layout and add to the document
loadrow = row(datatext, load_button, sizing_mode="stretch_width")
url_row = row(url_prefix_text, url_report_select, url_inst_select,
              url_binning_select, url_roi_select,
              url_startdate_picker, Div(text='-'), url_enddate_picker,
              url_build_button,)
loadblock = column(statustext, url_row, loadrow, sizing_mode="stretch_width")
selectrow = row(group_select, plot_select, plot_default_button)

curdoc().add_root(column(loadblock, selectrow, p))
