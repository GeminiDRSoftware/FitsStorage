#!/usr/bin/env python
"""
Plot nightly GMOS imaging zeropoints from the archive monitoring endpoint.

For each site (GMOS-N / GMOS-S) and each filter, this fetches the zeropoint
monitoring table, e.g.
  https://archive.gemini.edu/monitoring/zeropoint/canonical/GMOS-N/
      processing_tag=GMOS-N_IM-2/notengineering/filter=g
and plots them all on one bokeh HTML page, one plot per site/filter, with
night (UT date) on the x axis.

Clouds only ever make the zeropoint worse (lower), so the distribution of
measurements is asymmetric and we want the clear-sky (upper) value, without
being fooled by noise or spurious "way off" values.

Known events (mirror coatings, washes, detector changes - the *_EVENTS dicts)
cause jumps in the zeropoint. The data are split into segments between events
and each segment is treated independently. Within a segment we expect the
clear-sky zeropoint to decrease slowly and linearly with time, so we fit a
line to it:

1. Row cuts (filter_rows): require enough stars (MCC_NSAM) and drop QA Fail.
2. Per segment (fit_segment), iterate:
   - Compute each frame's residual from the current line, in units of its
     noise sigma = sqrt((MCC_ZPSD/sqrt(MCC_NSAM))**2 + floor**2).
   - Keep frames within -k_lo to +k_hi sigma. The asymmetric window rejects
     cloud-affected frames (below) and spurious high frames (above).
   - Combine the kept frames from each night into a weighted mean nightly
     value, so that nights with many frames don't dominate the fit.
   - Least-squares fit a line to the nightly values. The slope is not allowed
     to be greater than max_slope (default 0, ie no increase with time) - if
     it would be, the slope is fixed at max_slope and only the level fitted.
   until the set of kept frames stops changing. The starting line is a robust
   (Theil-Sen) fit to the "anchor" nights - nights with several frames that
   agree closely, which are very likely photometric - so the fit follows the
   level of the majority of stable nights rather than a few high nights, and
   gross outliers can't drag it. If there are too few anchor nights, it starts
   from a robust fit to an upper percentile of each night's values instead.
3. Nights with no kept frames are flagged cloudy (or outlier, if their frames
   were rejected high).
"""

import datetime
import io
import math
import os
from argparse import ArgumentParser

import numpy as np
import pandas as pd
import requests
from bokeh.io import output_file, save, show
from bokeh.layouts import column
from bokeh.models import (ColumnDataSource, CustomJS, HoverTool, Label, Span,
                          Whisker)
from bokeh.plotting import figure

USER_AGENT = 'GeminiInstMon'

INSTRUMENTS = ['GMOS-N', 'GMOS-S']
DEFAULT_FILTERS = ['g', 'r', 'i', 'z']

# These were from Teo by Email 20261106.
GMOS_N_EVENTS = {
'20000421': 'M1 coating (Al)',
'20040111': 'M1 coating (Al)',
'20041126': 'M1, M2, M3, Periscope coating',
'20050929': 'M1 insitu wash',
'20080728': 'M1 coating',
'20081022': 'M2 coating',
'20090707': 'M3 coating',
'20090710': 'M1 insitu wash',
'20100725': 'M3 coating',
'20100802': 'M1 insitu wash',
'20111020': 'e2v installed',
'20120107': 'M2 coating',
# '20130618': 'Periscope coating', # we don't want this one to trigger a new fit
'20130923': 'M1 coating',
'20150925': 'M2 coating',
'20170220': 'Hamamatsu installed',
'20200819': 'M1 contamination (was 20200812)',
'20201023': 'M1 insitu wash',
'20210825': 'M2 coating',
'20221020': 'M1 incident',
'20230430': 'M1 coating',
}

GMOS_S_EVENTS = {}

EVENTS = {'GMOS-N': GMOS_N_EVENTS, 'GMOS-S': GMOS_S_EVENTS}

# Night label offsets, matching fits_storage.db.selection.query_selection:
# GN night D spans UT [D 00:00, D+1 00:00) (14:00 HST boundary), so the night
# label is just the UT date. GS night D spans UT [D-1 18:00, D 18:00), so the
# night label is the date of (UT + 6h). (CHILE_OFFSET in
# fits_storage.gemini_metadata_utils is -6h and the night is UT - CHILE_OFFSET)
# Event dates are night labels: an event with date D happens before night D,
# so night D is the first night after the event (night >= event date).
NIGHT_OFFSET = {'GMOS-N': datetime.timedelta(hours=0),
                'GMOS-S': datetime.timedelta(hours=6)}


# ---------------------------------------------------------------------------
# Row filtering. Defaults here, most can be overridden on the command line.
# ---------------------------------------------------------------------------
MIN_NSAM = 5            # Minimum MCC_NSAM (number of stars measured). Frames
                        # with very few stars give most of the spurious values
MAX_ZPSD = None         # Maximum MCC_ZPSD (mag), None to disable
REJECT_QASTATES = ['Fail']


def filter_rows(df, min_nsam=MIN_NSAM):
    """
    Apply row-level selection criteria. Returns the filtered dataframe and
    prints how many rows each criterion removed.
    """
    criteria = [
        ('MCC_ZP present', df['MCC_ZP'].notna()),
        (f'MCC_NSAM >= {min_nsam}', df['MCC_NSAM'] >= min_nsam),
        (f'qastate not in {REJECT_QASTATES}',
         ~df['qastate'].isin(REJECT_QASTATES)),
    ]
    if MAX_ZPSD is not None:
        criteria.append((f'MCC_ZPSD <= {MAX_ZPSD}', df['MCC_ZPSD'] <= MAX_ZPSD))

    mask = pd.Series(True, index=df.index)
    for name, crit in criteria:
        newmask = mask & crit
        print(f"    {name}: removed {(mask & ~newmask).sum()} rows")
        mask = newmask
    return df[mask].sort_values('ut_datetime').copy()


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------
def build_url(server, instrument, processing_tag, filt):
    return '/'.join([server, 'monitoring', 'zeropoint', 'canonical',
                     instrument, f'processing_tag={processing_tag}',
                     'notengineering', f'filter={filt}'])


def fetch_table(url, cache_dir=None):
    """
    Fetch the tab-separated monitoring table into a dataframe. If cache_dir is
    given, re-use a previously fetched copy if there is one.
    """
    text = None
    cache_file = None
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)
        cache_file = os.path.join(
            cache_dir, url.split('://', 1)[-1].replace('/', '_') + '.tsv')
        if os.path.exists(cache_file):
            print(f"    Using cached {cache_file}")
            with open(cache_file) as f:
                text = f.read()

    if text is None:
        print(f"    Fetching {url}")
        r = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=300)
        r.raise_for_status()
        text = r.text
        if cache_file:
            with open(cache_file, 'w') as f:
                f.write(text)

    # The header line is prefixed with '# ', and missing values are 'None'
    df = pd.read_csv(io.StringIO(text), sep='\t', header=0,
                     na_values=['None'])
    df.columns = [c.lstrip('#').strip() for c in df.columns]
    if df.empty:
        return df
    df['ut_datetime'] = pd.to_datetime(df['ut_datetime'], format='ISO8601')
    for col in ['MCC_ZP', 'MCC_ZPSD', 'MCC_NSAM']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


# ---------------------------------------------------------------------------
# Nights and segments
# ---------------------------------------------------------------------------
def add_night(df, instrument):
    df['night'] = (df['ut_datetime'] + NIGHT_OFFSET[instrument]).dt.normalize()
    return df


def event_dates(events):
    """Sorted array of event dates (datetime64) from an *_EVENTS dict."""
    return np.array(sorted(pd.to_datetime(list(events), format='%Y%m%d')),
                    dtype='datetime64[ns]')


def add_segment(df, events):
    """
    Add a 'segment' column: the number of events on or before each night's
    date. An event with date D happens before night D, so segment k runs from
    night (event k-1) up to the night before event k.
    """
    df['segment'] = np.searchsorted(event_dates(events),
                                    df['night'].to_numpy(), side='right')
    return df


# ---------------------------------------------------------------------------
# Fitting
# ---------------------------------------------------------------------------
def theil_sen(x, y):
    """Robust line fit: median of pairwise slopes. Returns (slope, icpt)."""
    if len(x) < 2 or np.ptp(x) == 0:
        return 0.0, float(np.median(y))
    i, j = np.triu_indices(len(x), 1)
    dx = x[j] - x[i]
    ok = dx != 0
    slope = float(np.median((y[j] - y[i])[ok] / dx[ok]))
    return slope, float(np.median(y - slope * x))


def nightly_means(nights, x, zp, sig, sel):
    """
    Weighted mean of the selected frames for each night. Returns a dataframe
    indexed by night with columns x, zp, err, n.
    """
    w = 1 / sig ** 2
    t = pd.DataFrame({'night': nights[sel], 'x': x[sel], 'wzp': (w * zp)[sel],
                      'w': w[sel]})
    g = t.groupby('night').agg(x=('x', 'first'), wzp=('wzp', 'sum'),
                              w=('w', 'sum'), n=('w', 'size'))
    return pd.DataFrame({'x': g['x'], 'zp': g['wzp'] / g['w'],
                         'err': 1 / np.sqrt(g['w']), 'n': g['n']})


def fit_line(nm, floor, can_slope, max_slope=None):
    """
    Weighted least-squares line through nightly values nm (from
    nightly_means). If can_slope is False, fit a constant. If the fitted
    slope is greater than max_slope (mag/yr), fix the slope at max_slope and
    fit just the intercept. Returns (slope, icpt, limited) where limited says
    whether max_slope was applied.
    """
    sig = np.sqrt(nm['err'] ** 2 + floor ** 2).to_numpy()
    w = 1 / sig ** 2
    x, y = nm['x'].to_numpy(), nm['zp'].to_numpy()
    slope = 0.0
    limited = False
    if can_slope:
        slope = float(np.polyfit(x, y, 1, w=1 / sig)[0])
        if max_slope is not None and slope > max_slope:
            slope, limited = max_slope, True
    return slope, float(np.sum(w * (y - slope * x)) / np.sum(w)), limited


def fit_segment(seg, floor=0.03, k_lo=2.0, k_hi=3.0, start_pct=80,
                min_nights_slope=5, min_span_days=60, anchor_min_frames=3,
                anchor_max_std=0.02, min_anchor_nights=3, max_slope=0.0,
                maxiters=30):
    """
    Fit the clear-sky zeropoint line to one segment of frames (see module
    docstring). x is in years since the first night of the segment.

    Returns (frame_flags, fit) where frame_flags is a dataframe indexed like
    seg with boolean columns 'used', 'low', 'high' and float 'resid', and fit
    is a dict describing the line.
    """
    nights = seg['night'].to_numpy()
    t0 = seg['night'].min()
    x = ((seg['night'] - t0).dt.days / 365.25).to_numpy()
    zp = seg['MCC_ZP'].to_numpy()
    err = (seg['MCC_ZPSD'] / np.sqrt(seg['MCC_NSAM'])).to_numpy()
    sig = np.sqrt(np.nan_to_num(err) ** 2 + floor ** 2)

    n_nights = seg['night'].nunique()
    span_days = (seg['night'].max() - t0).days
    can_slope = n_nights >= min_nights_slope and span_days >= min_span_days

    # Starting line. Preferably a robust fit to the "anchor" nights: nights
    # with several frames that agree closely with each other, which are very
    # likely photometric (clouds are rarely that uniform). Using these means
    # the fit follows the level of the majority of stable nights, rather than
    # latching on to a few high nights. If there are too few anchor nights,
    # fall back to a robust fit to an upper percentile of each night, lifted
    # so it sits in the upper part of the nightly distribution.
    stats = seg.groupby('night')['MCC_ZP'].agg(
        ['size', 'std', 'median', lambda v: v.quantile(start_pct / 100)])
    stats.columns = ['n', 'std', 'median', 'q']
    sx = ((stats.index - t0).days / 365.25).to_numpy()
    anchor = ((stats['n'] >= anchor_min_frames) &
              (stats['std'] <= anchor_max_std)).to_numpy()
    if anchor.sum() >= (min_anchor_nights if can_slope else 1):
        ax, ay = sx[anchor], stats['median'].to_numpy()[anchor]
        if can_slope:
            slope, icpt = theil_sen(ax, ay)
        else:
            slope, icpt = 0.0, float(np.median(ay))
        start = 'anchor'
    else:
        q = stats['q'].to_numpy()
        if can_slope:
            slope, icpt = theil_sen(sx, q)
        else:
            slope, icpt = 0.0, float(np.median(q))
        icpt += float(np.percentile(q - (icpt + slope * sx), 75))
        start = 'percentile'
    if max_slope is not None:
        slope = min(slope, max_slope)

    keep = None
    limited = False
    for _ in range(maxiters):
        d = (zp - (icpt + slope * x)) / sig
        new = (d > -k_lo) & (d < k_hi)
        if not new.any():
            break
        nm = nightly_means(nights, x, zp, sig, new)
        # Need enough nights left to fit a slope
        slope, icpt, limited = fit_line(nm, floor, can_slope and len(nm) >= 3,
                                        max_slope)
        if keep is not None and (new == keep).all():
            break
        keep = new
    if keep is None:
        keep = np.zeros(len(zp), bool)
        nm = nightly_means(nights, x, zp, sig, keep)

    resid = zp - (icpt + slope * x)
    d = resid / sig
    flags = pd.DataFrame({'used': keep, 'low': ~keep & (d <= 0),
                          'high': ~keep & (d > 0), 'resid': resid},
                         index=seg.index)
    nresid = nm['zp'] - (icpt + slope * nm['x'])
    fit = {
        't0': t0,
        'first_night': t0,
        'last_night': seg['night'].max(),
        'slope': slope,         # mag / year
        'icpt': icpt,           # mag at t0
        'sloped': can_slope,
        'start': start,
        'slope_limited': limited,
        'n_anchor': int(anchor.sum()),
        'n_nights': n_nights,
        'n_nights_used': len(nm),
        'n_frames': len(seg),
        'n_frames_used': int(keep.sum()),
        'rms': float(np.sqrt(np.mean(nresid ** 2))) if len(nm) else np.nan,
    }
    return flags, fit


def fit_segments(df, events, **kwargs):
    """
    Split df into segments between events and fit each. Adds 'segment',
    'used', 'low', 'high', 'resid' columns to df. Returns (nightly, fits)
    dataframes.
    """
    df = add_segment(df, events)
    ev = event_dates(events)
    fits = []
    flags = []
    for seg_id, seg in df.groupby('segment'):
        f, fit = fit_segment(seg, **kwargs)
        flags.append(f)
        fit['segment'] = seg_id
        fit['start_event'] = pd.Timestamp(ev[seg_id - 1]) if seg_id > 0 \
            else None
        fit['end_event'] = pd.Timestamp(ev[seg_id]) if seg_id < len(ev) \
            else None
        fits.append(fit)
    flags = pd.concat(flags)
    for col in flags.columns:
        df[col] = flags[col]
    fits = pd.DataFrame(fits)
    return df, nightly_values(df, fits, kwargs.get('floor', 0.03)), fits


def nightly_values(df, fits, floor):
    """
    One row per night. Clear nights (with used frames) get the weighted mean
    of their used frames. Other nights get the median of their frames, for
    display only, and are flagged 'cloudy' or 'outlier'.
    """
    rows = []
    fits = fits.set_index('segment')
    for night, grp in df.groupby('night'):
        used = grp[grp['used']]
        seg = grp['segment'].iloc[0]
        fit = fits.loc[seg]
        x = (night - fit['t0']).days / 365.25
        row = {'night': night, 'segment': seg,
               'fit': fit['icpt'] + fit['slope'] * x,
               'n_used': len(used), 'n_low': int(grp['low'].sum()),
               'n_high': int(grp['high'].sum()),
               'n_files': grp['filename'].nunique()}
        if len(used):
            err = (used['MCC_ZPSD'] / np.sqrt(used['MCC_NSAM'])).fillna(0)
            w = 1 / (err ** 2 + floor ** 2)
            row['zp'] = np.sum(w * used['MCC_ZP']) / np.sum(w)
            row['zp_err'] = 1 / np.sqrt(np.sum(w))
            row['status'] = 'clear'
        else:
            row['zp'] = grp['MCC_ZP'].median()
            row['zp_err'] = np.nan
            row['status'] = 'cloudy' if row['n_low'] >= row['n_high'] \
                else 'outlier'
        rows.append(row)
    ndf = pd.DataFrame(rows)
    ndf['resid'] = ndf['zp'] - ndf['fit']
    return ndf


def print_fits(fits):
    print("    Seg  First night  Last night   Nights(used)  "
          "ZP at start  Slope(mag/yr)   RMS")
    for _, f in fits.iterrows():
        slope = f"{f['slope']:+.4f}" if f['sloped'] else "  (const)"
        if f['slope_limited']:
            slope += '*'
        print(f"    {f['segment']:3d}  {f['first_night']:%Y-%m-%d}   "
              f"{f['last_night']:%Y-%m-%d}   {f['n_nights']:5d}({f['n_nights_used']:4d})"
              f"    {f['icpt']:7.3f}     {slope:>10}   {f['rms']:.3f}")
    if fits['slope_limited'].any():
        print("    * fitted slope was > --max-slope, so slope fixed at that")


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------
def make_plot(instrument, filt, df, ndf, fits, events):
    p = figure(title=f"{instrument} {filt}-band nightly clear-sky zeropoint "
                     f"(MCC_ZP)",
               x_axis_type='datetime', x_axis_label='Night (UT date)',
               y_axis_label='Zeropoint (mag)', width=1400, height=800,
               tools='pan,box_zoom,wheel_zoom,reset,save')
    # Don't let hidden layers (eg wild outliers) set the ranges
    p.y_range.only_visible = True
    p.x_range.only_visible = True

    # Events: labelled vertical lines. Stagger the labels of events close
    # together in time so they don't overprint.
    prev_t, label_y = None, 5
    for date, desc in sorted(events.items()):
        t = pd.Timestamp(date)
        if prev_t is not None and (t - prev_t).days < 90:
            label_y = label_y + 170 if label_y < 300 else 5
        else:
            label_y = 5
        prev_t = t
        p.add_layout(Span(location=t, dimension='height', line_color='firebrick',
                          line_dash='dashed', line_alpha=0.6))
        p.add_layout(Label(x=t, y=label_y, y_units='screen', angle=math.pi / 2,
                           text=f"{t:%Y-%m-%d} {desc}", text_font_size='8pt',
                           text_color='firebrick', text_alpha=0.8,
                           x_offset=-3))

    # Individual frames. These start hidden - click the legend to show them.
    cols = ['night', 'MCC_ZP', 'MCC_ZPSD', 'MCC_NSAM', 'filename', 'qastate',
            'resid']
    frame_renderers = []
    for sel, label, kw in [
            (df['used'], 'Frames: used',
             dict(size=4, color='grey', alpha=0.5)),
            (df['low'], 'Frames: rejected low',
             dict(size=4, color='orange', alpha=0.6)),
            (df['high'], 'Frames: rejected high',
             dict(size=6, marker='x', color='red', alpha=0.8))]:
        r = p.scatter('night', 'MCC_ZP', source=ColumnDataSource(df[sel][cols]),
                      legend_label=label, **kw)
        r.visible = False
        frame_renderers.append(r)

    # Segment fit lines
    fsrc = ColumnDataSource(fits.assign(
        y0=fits['icpt'],
        y1=fits['icpt'] + fits['slope'] *
        (fits['last_night'] - fits['t0']).dt.days / 365.25))
    r_fit = p.segment(x0='first_night', y0='y0', x1='last_night', y1='y1',
                      source=fsrc, line_color='green', line_width=2,
                      legend_label='Segment fit')

    # Nightly points. Clear: filled with error whiskers. Others: hollow,
    # and hidden by default.
    night_renderers = []
    for status, kw in [
            ('clear', dict(color='navy')),
            ('cloudy', dict(line_color='grey', fill_color=None)),
            ('outlier', dict(line_color='red', fill_color=None))]:
        sub = ndf[ndf['status'] == status]
        src = ColumnDataSource(sub.assign(lower=sub['zp'] - sub['zp_err'],
                                          upper=sub['zp'] + sub['zp_err']))
        r = p.scatter('night', 'zp', source=src, size=6,
                      legend_label=f'Night: {status}', **kw)
        night_renderers.append(r)
        if status == 'clear':
            w = Whisker(source=src, base='night', lower='lower',
                        upper='upper', line_color='navy', line_alpha=0.6)
            p.add_layout(w)
            # Hide the whiskers along with the points
            r.js_on_change('visible', CustomJS(
                args=dict(w=w), code="w.visible = cb_obj.visible"))
        else:
            r.visible = False

    p.add_tools(HoverTool(renderers=night_renderers, tooltips=[
        ('Night', '@night{%F}'), ('ZP', '@zp{0.000} ± @zp_err{0.000}'),
        ('Fit', '@fit{0.000}'), ('Resid', '@resid{+0.000}'),
        ('Status', '@status'),
        ('used / low / high', '@n_used / @n_low / @n_high')],
        formatters={'@night': 'datetime'}))
    p.add_tools(HoverTool(renderers=frame_renderers, tooltips=[
        ('File', '@filename'), ('ZP', '@MCC_ZP{0.000}'),
        ('Resid', '@resid{+0.000}'),
        ('ZPSD', '@MCC_ZPSD{0.000}'), ('NSAM', '@MCC_NSAM'),
        ('QA', '@qastate')]))
    p.add_tools(HoverTool(renderers=[r_fit], tooltips=[
        ('Segment', '@segment: @first_night{%F} to @last_night{%F}'),
        ('ZP at start', '@icpt{0.000}'), ('Slope', '@slope{+0.0000} mag/yr'),
        ('Slope limited', '@slope_limited'),
        ('Start / anchor nights', '@start / @n_anchor'),
        ('Nights (used)', '@n_nights (@n_nights_used)'), ('RMS', '@rms{0.000}')],
        formatters={'@first_night': 'datetime', '@last_night': 'datetime'},
        line_policy='interp'))

    # Put the legend outside the plot area so it can't hide points
    p.legend.click_policy = 'hide'
    p.add_layout(p.legend[0], 'right')
    return p


def main():
    parser = ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument('--server', default='https://archive.gemini.edu',
                        help='Archive server to query')
    parser.add_argument('--instruments', nargs='+', default=INSTRUMENTS,
                        choices=INSTRUMENTS, help='Instruments to plot')
    parser.add_argument('--filters', nargs='+', default=DEFAULT_FILTERS,
                        help='Filters to plot')
    parser.add_argument('--processing-tag', default='{instrument}_IM-2',
                        help='Processing tag; {instrument} is substituted')
    parser.add_argument('--min-nsam', type=int, default=MIN_NSAM,
                        help='Minimum number of stars (MCC_NSAM) per frame')
    parser.add_argument('--floor', type=float, default=0.03,
                        help='Noise floor (mag) added in quadrature to each '
                             'frame and nightly error')
    parser.add_argument('--k-lo', type=float, default=2.0,
                        help='Reject frames more than this many sigma below '
                             'the segment fit')
    parser.add_argument('--k-hi', type=float, default=3.0,
                        help='Reject frames more than this many sigma above '
                             'the segment fit')
    parser.add_argument('--start-pct', type=float, default=80,
                        help='Percentile of each night used for the robust '
                             'starting fit')
    parser.add_argument('--min-nights-slope', type=int, default=5,
                        help='Fit a constant, not a slope, to segments with '
                             'fewer nights than this')
    parser.add_argument('--min-span-days', type=int, default=60,
                        help='Fit a constant, not a slope, to segments '
                             'spanning fewer days than this')
    parser.add_argument('--anchor-max-std', type=float, default=0.02,
                        help='Nights with at least 3 frames and frame-to-frame '
                             'std below this (mag) are anchors for the '
                             'starting fit')
    parser.add_argument('--max-slope', type=float, default=0.0,
                        help='Maximum allowed slope (mag/yr) of segment fits. '
                             'Default 0: the zeropoint may not increase '
                             'with time within a segment')
    parser.add_argument('--outfile', default='gmos_zp.html',
                        help='HTML file to write all the plots to')
    parser.add_argument('--cache-dir', default=None,
                        help='Cache fetched tables here and re-use them')
    parser.add_argument('--show', action='store_true',
                        help='Open each plot in a browser')
    args = parser.parse_args()

    plots = []
    for instrument in args.instruments:
        ptag = args.processing_tag.format(instrument=instrument)
        events = EVENTS[instrument]
        for filt in args.filters:
            print(f"{instrument} {filt}:")
            url = build_url(args.server, instrument, ptag, filt)
            df = fetch_table(url, cache_dir=args.cache_dir)
            print(f"    Fetched {len(df)} rows")
            if df.empty:
                continue
            df = filter_rows(df, min_nsam=args.min_nsam)
            if df.empty:
                print("    No rows left after filtering")
                continue
            df = add_night(df, instrument)
            df, ndf, fits = fit_segments(
                df, events, floor=args.floor, k_lo=args.k_lo, k_hi=args.k_hi,
                start_pct=args.start_pct,
                min_nights_slope=args.min_nights_slope,
                min_span_days=args.min_span_days,
                anchor_max_std=args.anchor_max_std,
                max_slope=args.max_slope)
            print(f"    {len(df)} frames: {int(df['used'].sum())} used, "
                  f"{int(df['low'].sum())} rejected low, "
                  f"{int(df['high'].sum())} rejected high")
            print("    Nights: " + ", ".join(
                f"{n} {k}" for k, n in ndf['status'].value_counts().items()))
            print_fits(fits)

            plots.append(make_plot(instrument, filt, df, ndf, fits, events))

    if not plots:
        print("No plots to write")
        return
    # All the plots, one above the other, on one page
    outdir = os.path.dirname(args.outfile)
    if outdir:
        os.makedirs(outdir, exist_ok=True)
    output_file(args.outfile, title="GMOS zeropoints")
    # Link the time axes, so panning / zooming one plot does the same to all.
    # The y axes stay independent as the zeropoints differ between filters.
    for p in plots[1:]:
        p.x_range = plots[0].x_range
    layout = column(*plots)
    if args.show:
        show(layout)
    else:
        save(layout)
    print(f"Wrote {len(plots)} plots to {args.outfile}")


if __name__ == '__main__':
    main()
