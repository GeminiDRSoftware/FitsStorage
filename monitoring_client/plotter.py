import pandas as pd
import copy
from math import pi

from bokeh.plotting import figure
from bokeh.layouts import column, row
from bokeh.models import Button, TextInput, Select, HoverTool, \
    ColumnDataSource, CategoricalColorMapper, DatePicker, Div, \
    BasicTickFormatter, BoxSelectTool, Legend

class InstMonPlot(object):
    def __init__(self):
        # create a plot with a title and axis labels
        self.fig = figure(title="Instrument monitoring",
                          x_axis_label='Data Label',
                          y_axis_label='Statistic',
                          height=512,
                          width=1024,
                          )
        self.fig.yaxis.formatter = BasicTickFormatter(use_scientific=False)
        self.fig.xaxis.ticker.desired_num_ticks=10

        # Add a legend, *outside* of the plot area, as otherwise it can hide points
        self.fig.add_layout(Legend(), 'right')

        # Color mapper for qastate
        mapper = CategoricalColorMapper(
            palette=["red", "orange", "blue", "green"],
            factors=["Fail", "Usable", "Undefined", "Pass"])

        # Add the scatter plot, with dummy data but configure the color mapper
        self.cds = ColumnDataSource(data=dict(x=[], y=[], qastate=[]))
        self.scatter = self.fig.scatter(
            x='x', y='y', source=self.cds,
            legend_field='qastate',
            fill_color={"field": "qastate", "transform": mapper},
            size=8, line_color=None)

        # Due to some bokeh bug(?) we need to explicitly create and update the
        # selection and nonselection glyphs if we want them to display properly
        # - because we use column names other than 'x' and 'y'.
        self.scatter.selection_glyph = self.scatter.glyph.clone(fill_alpha=1.0)
        self.scatter.nonselection_glyph = self.scatter.glyph.clone(fill_alpha=0.1, fill_color="grey")

        # Add the Hover tool, we configure it in filter_data
        self.fig.add_tools(HoverTool())

        # Add the selection tools
        self.fig.add_tools(BoxSelectTool())


        # Initialize empty data values
        self.df = []
        self.df = []
        self.plots = []
        self.scratch = set()

        self.create_widgets()

    def create_widgets(self):

        # Instantiate widgets
        self.datasource_text = TextInput(prefix="Data Source: ", sizing_mode="stretch_width")
        self.status_text = TextInput(prefix="Status:", sizing_mode="stretch_width")
        self.url_prefix_text = TextInput(prefix="URL builder:", sizing_mode="fixed",
                            width=340,
                            value="https://archive.gemini.edu/monitoring")
        self.url_report_select = Select(options=["checkBias", "checkFlat"])
        self.url_inst_select = Select(options=["GMOS-N", "GMOS-S"])
        self.url_binning_select = Select(options=[None, "1x1", "1x2", "1x4", "2x1", "2x2", "2x4", "4x1", "4x2", "4x4"])
        self.url_roi_select = Select(options=[None, "fullframe", "centralspectrum"])
        self.url_speed_select = Select(options=[None, "slow", "fast"])
        self.url_gain_select = Select(options=[None, "low", "high"])
        self.url_startdate_picker = DatePicker()
        self.url_enddate_picker = DatePicker()
        self.url_build_button = Button(label="Build URL")
        self.url_example_button = Button(label="Example URL")
        self.url_buildload_button = Button(label="Build & Load")
        self.load_button = Button(label="Load")
        self.group_select = Select(title="Group by", options=['None', 'max'], value='None')
        self.plot_select = Select(title="Plot", options=self.plots, value='Any')
        self.plot_default_button = Button(label="Default", align='end')

        self.selectnone_button = Button(label="Select None")
        self.selectpass_button = Button(label="Select Pass")
        self.selectfail_button = Button(label="Select Fail")
        self.selectundef_button = Button(label="Select Undefined")
        self.selectusable_button = Button(label="Select Usable")


        load_row = row(self.datasource_text, self.url_buildload_button,
                       self.load_button, sizing_mode="stretch_width")
        url_row = row(self.url_prefix_text, self.url_report_select,
                      self.url_inst_select, self.url_binning_select,
                      self.url_roi_select, self.url_speed_select,
                      self.url_gain_select, self.url_startdate_picker,
                      Div(text='-'), self.url_enddate_picker,
                      self.url_build_button, self.url_example_button)
        load_col = column(self.status_text, url_row, load_row, sizing_mode="stretch_width")
        select_row = row(self.group_select, self.plot_select,
                         self.plot_default_button)
        fig_buttons_col = column(Div(), self.selectnone_button,
                                 self.selectpass_button,
                                 self.selectfail_button,
                                 self.selectundef_button,
                                 self.selectusable_button)
        fig_row = row(self.fig, fig_buttons_col)
        self.element = column(load_col, select_row, fig_row)

        # Set callback functions
        self.load_button.on_event('button_click', self.load_data)
        self.url_build_button.on_event('button_click', self.build_url)
        self.url_buildload_button.on_event('button_click', self.buildload)
        self.url_example_button.on_event('button_click', self.example_url)

        self.plot_default_button.on_event('button_click', self.plot_default_callback)

        self.group_select.on_change('value', self.select_callback)
        self.plot_select.on_change('value', self.plot_callback)

        self.selectnone_button.on_event('button_click', self.select_none_callback)
        self.selectpass_button.on_event('button_click', self.select_pass_callback)
        self.selectfail_button.on_event('button_click', self.select_fail_callback)
        self.selectundef_button.on_event('button_click', self.select_undef_callback)
        self.selectusable_button.on_event('button_click', self.select_usable_callback)

    # Define Callback functions
    def build_url(self):
        select_assist = None
        if self.url_report_select.value == 'checkBias':
            select_assist = 'BIAS/DayCal/Raw'
        elif self.url_report_select.value == 'checkFlat':
            select_assist = 'OBJECT/DayCal/Raw/object=Twilight'
        all_items = [self.url_prefix_text.value, self.url_report_select.value,
                     select_assist, self.url_inst_select.value,
                     self.url_binning_select.value, self.url_roi_select.value,
                     self.url_speed_select.value, self.url_gain_select.value]
        if self.url_startdate_picker.value and self.url_enddate_picker.value:
            all_items.append(f"{self.url_startdate_picker.value.replace('-', '')}-"
                             f"{self.url_enddate_picker.value.replace('-', '')}")
        elif self.url_startdate_picker.value:
            all_items.append(self.url_startdate_picker.value.replace('-', ''))
        items = [i for i in all_items if i]
        self.datasource_text.value = '/'.join(items)

    def example_url(self):
        self.datasource_text.value = 'https://archive.gemini.edu/monitoring/checkBias/BIAS/DayCal/Raw/GMOS-N/1x1/fullframe/slow/low/20250101-20250731'

    def load_data(self):
        print(f"loading {self.datasource_text.value} into fdf")
        self.fdf = pd.read_csv(self.datasource_text.value, sep='\t', header=0)
        self.fdf['ut_datetime'] = pd.to_datetime(self.fdf['ut_datetime'], format='ISO8601')
        self.fdf.sort_values(by=['ut_datetime', 'adid'], inplace=True)

        self.plot_select.options = list(self.fdf.columns)

        print(f"Loaded {len(self.fdf)} rows")
        self.status_text.value = f"Loaded {len(self.fdf)} rows."

        self.filter_data()

    def buildload(self):
        self.build_url()
        self.load_data()
        
    def filter_data(self):
        self.df = copy.copy(self.fdf)
        print(f"Starting filter_data, {len(self.df)=}")

        if self.group_select.value == 'max':
            self.df = self.df.groupby('data_label', as_index=False).max()
        print(f"Post-group {len(self.df)=}")

        self.df['row_number'] = self.df.reset_index().index
        self.df = self.df.set_index('row_number')
        print(f"Filtered down to {len(self.df)} rows.")
        self.status_text.value = f"Filtered down to {len(self.df)} rows."

        self.cds.data = self.df

        labels = dict(self.df['data_label'])
        for i in labels.keys():
            dlsplit = labels[i].split('-')
            labels[i]=''
            if len(dlsplit) == 4:
                labels[i] = dlsplit[1]
        self.fig.xaxis.major_label_overrides = labels
        self.fig.xaxis.major_label_orientation = pi/4

        self.fig.hover.tooltips=[("DL", "@data_label"), ("ext", "@adid"),
                   ("qastate", "@qastate")]

    def select_callback(self, attr, old, new):
        self.filter_data()

    def plot_callback(self, attr, old, new):
        self.scatter.glyph.update(x = 'row_number', y = new)
        self.scatter.selection_glyph.update(x = 'row_number', y = new)
        self.scatter.nonselection_glyph.update(x = 'row_number', y = new)

        self.fig.yaxis.axis_label = new

        self.status_text.value = f"Plotted {self.scatter.glyph.y}"

    def plot_default_callback(self):
        if self.url_report_select.value == 'checkBias':
            self.group_select.value = ''
            self.plot_select.value = 'OSCOMED'
        elif self.url_report_select.value == 'checkFlat':
            self.group_select.value = ''
            self.plot_select.value = 'FLATMED'

    def select_none_callback(self):
        self.scatter.data_source.selected.indices = []

    def select_pass_callback(self):
        return self.select_by_qastate('Pass')

    def select_fail_callback(self):
        return self.select_by_qastate('Fail')

    def select_undef_callback(self):
        return self.select_by_qastate('Undefined')

    def select_usable_callback(self):
        return self.select_by_qastate('Usable')

    def select_by_qastate(self, qastate):
        self.scatter.data_source.selected.indices = []
        new_indices = [i for i, qa in enumerate(self.scatter.data_source.data["qastate"]) if qa == qastate]
        self.scatter.data_source.selected.indices = new_indices
