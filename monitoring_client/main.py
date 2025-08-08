from bokeh.plotting import curdoc
from bokeh.layouts import column

from .plotter import InstMonPlot
from .qaset import InstMonQA

imp = InstMonPlot()
page = imp.element

if True:
    imqa = InstMonQA(imp.scatter)
    page = column(imp.element, imqa.element)
curdoc().add_root(page)
