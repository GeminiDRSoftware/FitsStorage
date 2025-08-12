from bokeh.layouts import column, row
from bokeh.models import Button, TextAreaInput, Div, Select

from fits_storage.gemini_metadata_utils.progid_obsid_dl import GeminiDataLabel, GeminiObservation

import requests

class InstMonQA(object):
    def __init__(self, scatter, status_text):
        self.scatter = scatter
        self.status_text = status_text

        self.scratch = set()
        self.files = []

        self.create_widgets()

    def create_widgets(self):
        self.scratch_textarea = TextAreaInput(cols=24, rows=20, title='ScratchPad')
        self.copy_select_button = Button(label="Get Data Labels")
        self.get_obsids_button = Button(label="Convert to Obs-IDs")
        self.show_scratch_button = Button(label="Show on Plot")
        self.get_files_button = Button(label="Get Filenames")
        self.server_select = Select(options=['mkofits-lv1', 'cpofits-lv1', 'hbffitstape-lp2', 'archive'])

        self.filenames_textarea = TextAreaInput(cols=24, rows=20, title='File Names')
        self.fileinfo_textarea = TextAreaInput(cols=80, rows=20, title='File Info')


        scratch_col = column(Div(), self.copy_select_button,
                         self.get_obsids_button, self.show_scratch_button,
                             Div(), self.server_select, self.get_files_button)
        qarow = row(self.scratch_textarea, scratch_col,
                    self.filenames_textarea, self.fileinfo_textarea)

        self.element = qarow

        # Set callback functions
        self.copy_select_button.on_event('button_click', self.copy_selected_callback)
        self.get_obsids_button.on_event('button_click', self.get_obsids_callback)
        self.show_scratch_button.on_event('button_click', self.show_scratch_callback)
        self.get_files_button.on_event('button_click', self.get_files_callback)

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

    def get_files_callback(self):
        server = self.server_select.value
        scheme = 'http://'
        if server == 'archive':
            server = 'archive.gemini.edu'
            scheme = 'https://'
        alljqas = []
        for item in self.scratch:
            url = f"{scheme}{server}//jsonqastate/present/RAW/Raw/{item}"
            r = requests.get(url)
            if r.status_code != 200:
                self.status_text.value = f"Bad http status {r.status_code} for {url}."
            jqas = r.json()

            if len(jqas) == 0:
                self.status_text.value = f"Got no files for {item}"

            alljqas.extend(jqas)
        # Make a self.files a dictionary keyed by filename of dictionaries of data_label and qa_state
        self.files = {}
        for jqa in alljqas:
            self.files[jqa['filename']] = {'data_label': jqa['data_label'], 'qa_state': jqa['qa_state']}

        self.filenames_textarea.value = ''
        self.fileinfo_textarea.value=''

        for fn in sorted(self.files.keys()):
            self.filenames_textarea.value += f"{fn}\n"
            self.fileinfo_textarea.value +=  (f"{fn}\t"
                                              f"{self.files[fn]['data_label']}\t"
                                              f"{self.files[fn]['qa_state']}\n")
