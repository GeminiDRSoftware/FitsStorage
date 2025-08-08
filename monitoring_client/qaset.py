from bokeh.layouts import column, row
from bokeh.models import Button, TextAreaInput, Div, Selection

from fits_storage.gemini_metadata_utils.progid_obsid_dl import GeminiDataLabel, GeminiObservation

class InstMonQA(object):
    def __init__(self, scatter):
        self.scatter = scatter

        self.scratch = set()

        self.create_widgets()

    def create_widgets(self):
        self.scratch_textarea = TextAreaInput(cols=30, rows=20, title='ScratchPad')
        self.copy_select_button = Button(label="Get Data Labels")
        self.get_obsids_button = Button(label="Convert to Obs-IDs")
        self.show_scratch_button = Button(label="Show on Plot")

        markcol = column(Div(), self.copy_select_button,
                         self.get_obsids_button, self.show_scratch_button)
        markrow = row(self.scratch_textarea, markcol)

        self.element = markrow

        # Set callback functions
        self.copy_select_button.on_event('button_click', self.copy_selected_callback)
        self.get_obsids_button.on_event('button_click', self.get_obsids_callback)
        self.show_scratch_button.on_event('button_click', self.show_scratch_callback)

    # Define callback functions
    def copy_selected_callback(self):
        for i in self.scatter.data_source.selected.indices:
            self.scratch.add(self.scatter.data_source.data['data_label'][i])
            self.scratch_textarea.value = ''
            scratchlist = list(self.scratch)
            scratchlist.sort()
            for i in scratchlist:
                self.scratch_textarea.value += f"{i}\n"

    def get_obsids_callback(self):
        obsids = set()
        for i in self.scratch:
            dl = GeminiDataLabel(i)
            if dl.valid:
                obsids.add(dl.observation_id)
                continue
            oid = GeminiObservation(i)
            if oid.valid:
                obsids.add(oid.observation_id)
                continue
            obsids.add(f"# INVALID: {i}")
        self.scratch = obsids
        self.scratch_textarea.value = ''
        scratchlist = list(self.scratch)
        scratchlist.sort()
        for i in scratchlist:
            self.scratch_textarea.value += f"{i}\n"

    def show_scratch_callback(self):
        self.scatter.data_source.selected.indices = []
        new_indices = []
        for obsid in self.scratch:
            new_indices.extend([i for i, dl in enumerate(self.scatter.data_source.data["data_label"]) if dl.startswith(obsid)])
        self.scatter.data_source.selected.indices = new_indices