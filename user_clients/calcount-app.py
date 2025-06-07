from bokeh.layouts import column, row
from bokeh.models import (Button, TextInput, Select, ColumnDataSource,
                          DatePicker, Div, FactorRange)
from bokeh.plotting import figure, curdoc
import requests
import datetime

# create a plot with a title and axis labels
p = figure(title="File count",
           x_axis_label='UT date',
           y_axis_label='Count',
           height=512,
           width=1024,
           x_range=[]
           )

# Add the bar plot, with dummy data
cds = ColumnDataSource(data=dict(dates=[], counts=[]))
s = p.vbar(x='dates', top='counts', width=0.8, source=cds)
p.x_range = FactorRange(factors=[])
p.xaxis.major_label_orientation = "vertical"

# Initialize empty data values
data = []

# Add the widgets
datatext = TextInput(prefix="Data Source: ", sizing_mode="stretch_width")
statustext = TextInput(prefix="Status:", sizing_mode="stretch_width")
url_prefix_text = TextInput(prefix="URL builder:", sizing_mode="fixed",
                            width=340,
                            value="http://archive.gemini.edu/jsonsummary")
url_inst_select = Select(options=["GMOS-N", "GMOS-S"])
url_thing_select = Select(options=['Raw/RAW/BIAS',
                                   'Raw/RAW/dayCal/OBJECT/object=Twilight'])
url_binning_select = Select(options=[None, "1x1", "2x2"])
url_gain_select = Select(options=['high', 'low'])
url_speed_select = Select(options=['fast', 'slow'])
url_roi_select = Select(options=[None, "fullframe", "centralspectrum"])
url_startdate_picker = DatePicker()
url_enddate_picker = DatePicker()
url_build_button = Button(label="Build URL")
load_button = Button(label="Load")


# Define callback functions

def build_url():
    all_items = [url_prefix_text.value, url_inst_select.value,
                 url_thing_select.value,
                 url_binning_select.value, url_roi_select.value,
                 url_gain_select.value, url_speed_select.value]
    if url_startdate_picker.value and url_enddate_picker.value:
        all_items.append(f"{url_startdate_picker.value.replace('-', '')}-"
                         f"{url_enddate_picker.value.replace('-', '')}")
    elif url_startdate_picker.value:
        all_items.append(url_startdate_picker.value.replace('-', ''))
    items = [i for i in all_items if i]
    datatext.value = '/'.join(items)

def load_data():
    global cds
    global s
    global p

    print(f"loading {datatext.value}")
    r = requests.get(datatext.value)
    data = r.json()

    print(f"Loaded {len(data)} items")

    d = {}
    for item in data:
        date = datetime.datetime.fromisoformat(item['ut_datetime']).date()
        if date in d.keys():
            d[date] += 1
        else:
            d[date] = 1

    dateobjs = list(d.keys())
    dateobjs.sort()
    counts = [d[i] for i in dateobjs]
    dates = [str(i) for i in dateobjs]

    print(f"{len(dates)=} {len(counts)=}")
    print(f"{dates} {counts}")

    p.x_range = FactorRange(factors=dates)
    p.y_range.start = 0
    p.y_range.end = 10

    cds = ColumnDataSource(data=dict(dates=dates, counts=counts))
    s.data_source = cds


load_button.on_event('button_click', load_data)
url_build_button.on_event('button_click', build_url)


# put the button and plot in a layout and add to the document
loadrow = row(datatext, load_button, sizing_mode="stretch_width")
url_row = row(url_prefix_text, url_inst_select, url_thing_select,
              url_binning_select, url_gain_select, url_speed_select,
              url_roi_select,
              url_startdate_picker, Div(text='-'), url_enddate_picker,
              url_build_button,)
loadblock = column(statustext, url_row, loadrow, sizing_mode="stretch_width")

curdoc().add_root(column(loadblock, p))
