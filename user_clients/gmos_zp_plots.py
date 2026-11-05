#!/usr/bin/env python
"""
Plot nightly GMOS imaging zeropoints from the archive monitoring endpoint.

For each site (GMOS-N / GMOS-S) and each filter, this fetches the zeropoint
monitoring table, e.g.
  https://archive.gemini.edu/monitoring/zeropoint/canonical/GMOS-N/
      processing_tag=GMOS-N_IM-2/notengineering/filter=g
and writes one bokeh HTML plot per site/filter with night (UT date) on the x
axis.

Clouds only ever make the zeropoint worse (lower), so the distribution of
measurements is asymmetric and we want the clear-sky (upper) value, without
being fooled by noise or spurious "way off" values. The steps are:

1. Row cuts (filter_rows): require enough stars (MCC_NSAM), drop QA Fail, and
   reject gross outliers that are far from a rolling median over many months.
2. Per-night upper envelope (combine_nights): asymmetric iterative clip, tight
   below (cloudy frames) and looser above (spurious high frames), converging
   on the upper cluster of each night.
3. Cross-night envelope (flag_nights): compare each night with a rolling upper
   percentile of neighbouring nights, and flag nights that sit well below it as
   cloudy - on nights that were cloudy throughout, every frame is low and no
   per-night method can recover the clear-sky value.
"""

import datetime
import io
import os
from argparse import ArgumentParser

import numpy as np
import pandas as pd
import requests
from bokeh.io import output_file, save, show
from bokeh.models import ColumnDataSource, CustomJS, HoverTool, Whisker
from bokeh.plotting import figure

USER_AGENT = 'GeminiGmosZpPlots'

INSTRUMENTS = ['GMOS-N', 'GMOS-S']
DEFAULT_FILTERS = ['g', 'r', 'i', 'z']

# Night label offsets, matching fits_storage.db.selection.query_selection:
# GN night D spans UT [D 00:00, D+1 00:00) (14:00 HST boundary), so the night
# label is just the UT date. GS night D spans UT [D-1 18:00, D 18:00), so the
# night label is the date of (UT + 6h). (CHILE_OFFSET in
# fits_storage.gemini_metadata_utils is -6h and the night is UT - CHILE_OFFSET)
NIGHT_OFFSET = {'GMOS-N': datetime.timedelta(hours=0),
                'GMOS-S': datetime.timedelta(hours=6)}


# ---------------------------------------------------------------------------
# Row filtering. Defaults here, most can be overridden on the command line.
# ---------------------------------------------------------------------------
MIN_NSAM = 5            # Minimum MCC_NSAM (number of stars measured). Frames
                        # with very few stars give most of the spurious values
MAX_ZPSD = None         # Maximum MCC_ZPSD (mag), None to disable
REJECT_QASTATES = ['Fail']
SANITY_WINDOW_DAYS = 180    # Width of rolling median window for gross outliers
SANITY_MAX_DEV = 0.5        # Max |ZP - rolling median| (mag) to keep a frame
SANITY_MIN_FRAMES = 5       # Min frames in window for the sanity check to apply


def filter_rows(df, min_nsam=MIN_NSAM, sanity_window=SANITY_WINDOW_DAYS,
                sanity_max_dev=SANITY_MAX_DEV):
    """
    Apply row-level selection criteria. Prints how many rows each criterion
    removed. Returns (kept, sanity_rejected) dataframes - the latter are the
    otherwise-good frames rejected as gross outliers, kept so we can plot them.
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
    df = df[mask].sort_values('ut_datetime').copy()

    # Gross outlier rejection: compare each frame with a centred rolling
    # median of all frames within +/- sanity_window/2 days.
    roll = (df.set_index('ut_datetime')['MCC_ZP']
            .rolling(f'{sanity_window}D', center=True,
                     min_periods=SANITY_MIN_FRAMES).median())
    df['rolling_median'] = roll.to_numpy()
    bad = (df['MCC_ZP'] - df['rolling_median']).abs() > sanity_max_dev
    print(f"    |ZP - {sanity_window}d rolling median| <= {sanity_max_dev}: "
          f"removed {bad.sum()} rows")
    return df[~bad].copy(), df[bad].copy()


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
# Per-night combination
# ---------------------------------------------------------------------------
def add_night(df, instrument):
    df['night'] = (df['ut_datetime'] + NIGHT_OFFSET[instrument]).dt.normalize()
    return df


def upper_envelope(zp, err, floor=0.02, k_lo=2.0, k_hi=3.0, start_pct=80,
                   maxiters=20):
    """
    Estimate the clear-sky (upper cluster) value of a set of zeropoints.

    Each point gets a noise sigma = sqrt(err**2 + floor**2). Starting from the
    start_pct percentile, keep points within -k_lo to +k_hi sigma of the
    current estimate, recompute the estimate as the weighted mean of those,
    and iterate until the kept set stops changing. Points far below (cloudy)
    and far above (spurious) are rejected.

    Returns (estimate, error_on_estimate, keep, low, high) where keep, low and
    high are boolean arrays of points used, rejected low and rejected high.
    """
    sig = np.sqrt(np.nan_to_num(err) ** 2 + floor ** 2)
    c = np.percentile(zp, start_pct)
    keep = np.zeros(len(zp), bool)
    for _ in range(maxiters):
        d = (zp - c) / sig
        new = (d > -k_lo) & (d < k_hi)
        if not new.any():
            # Nothing near the estimate - fall back to the closest point
            new = np.zeros(len(zp), bool)
            new[np.argmin(np.abs(d))] = True
        w = 1 / sig[new] ** 2
        c = np.sum(w * zp[new]) / np.sum(w)
        if (new == keep).all():
            break
        keep = new
    d = (zp - c) / sig
    return c, 1 / np.sqrt(np.sum(w)), keep, ~keep & (d <= 0), ~keep & (d > 0)


def combine_nights(df, floor=0.02, k_lo=2.0, k_hi=3.0, start_pct=80):
    """
    Combine all rows in each night into one clear-sky estimate using
    upper_envelope(). Per-frame errors are MCC_ZPSD / sqrt(MCC_NSAM). Adds
    boolean 'clipped_low' and 'clipped_high' columns to df, and returns a
    per-night dataframe.
    """
    df['clipped_low'] = False
    df['clipped_high'] = False
    df['frame_err'] = df['MCC_ZPSD'] / np.sqrt(df['MCC_NSAM'])
    nights = []
    for night, grp in df.groupby('night'):
        zp = grp['MCC_ZP'].to_numpy()
        c, err, keep, low, high = upper_envelope(
            zp, grp['frame_err'].to_numpy(), floor=floor, k_lo=k_lo,
            k_hi=k_hi, start_pct=start_pct)
        df.loc[grp.index, 'clipped_low'] = low
        df.loc[grp.index, 'clipped_high'] = high
        nights.append({
            'night': night,
            'zp': c,
            'zp_err': err,
            'zp_median_all': np.median(zp),
            'zp_std': np.std(zp[keep], ddof=1) if keep.sum() > 1 else np.nan,
            'n_used': int(keep.sum()),
            'n_low': int(low.sum()),
            'n_high': int(high.sum()),
            'n_files': grp['filename'].nunique(),
        })
    return pd.DataFrame(nights)


def flag_nights(ndf, window=60, pct=80, threshold=0.1, min_nights=3):
    """
    Compare each night with a rolling upper envelope across nights: the pct
    percentile of nightly values within +/- window/2 days. Adds 'envelope' and
    'status' columns: 'clear' if the night is within threshold mag of the
    envelope (or above it), 'cloudy' if more than threshold below, and
    'isolated' if there are fewer than min_nights nights in the window.
    """
    ndf = ndf.sort_values('night').reset_index(drop=True)
    env = (ndf.set_index('night')['zp']
           .rolling(f'{window}D', center=True, min_periods=min_nights)
           .quantile(pct / 100))
    ndf['envelope'] = env.to_numpy()
    ndf['status'] = np.where(ndf['envelope'].isna(), 'isolated',
                             np.where(ndf['zp'] < ndf['envelope'] - threshold,
                                      'cloudy', 'clear'))
    return ndf


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------
def make_plot(instrument, filt, df, ndf, sanity_rej):
    p = figure(title=f"{instrument} {filt}-band nightly clear-sky zeropoint "
                     f"(MCC_ZP)",
               x_axis_type='datetime', x_axis_label='Night (UT date)',
               y_axis_label='Zeropoint (mag)', width=1200, height=500,
               tools='pan,box_zoom,wheel_zoom,reset,save')
    # Don't let hidden layers (eg wild outliers) set the y range
    p.y_range.only_visible = True
    p.x_range.only_visible = True

    # Individual frames. These start hidden - click the legend to show them.
    cols = ['night', 'MCC_ZP', 'MCC_ZPSD', 'MCC_NSAM', 'filename', 'qastate']
    frame_renderers = []
    for sel, label, kw in [
            (~df['clipped_low'] & ~df['clipped_high'], 'Frames: used',
             dict(size=4, color='grey', alpha=0.5)),
            (df['clipped_low'], 'Frames: clipped low',
             dict(size=4, color='orange', alpha=0.6)),
            (df['clipped_high'], 'Frames: clipped high',
             dict(size=6, marker='x', color='red', alpha=0.8))]:
        r = p.scatter('night', 'MCC_ZP', source=ColumnDataSource(df[sel][cols]),
                      legend_label=label, **kw)
        r.visible = False
        frame_renderers.append(r)
    r = p.scatter('night', 'MCC_ZP',
                  source=ColumnDataSource(sanity_rej[cols]),
                  legend_label='Frames: gross outliers', size=7,
                  marker='x', color='purple')
    r.visible = False
    frame_renderers.append(r)

    # Cross-night envelope
    p.line('night', 'envelope', source=ColumnDataSource(ndf),
           color='green', alpha=0.5, legend_label='Envelope')

    # Nightly points. Clear: filled with error whiskers. Others: hollow.
    night_renderers = []
    for status, kw in [
            ('clear', dict(color='navy')),
            ('isolated', dict(line_color='navy', fill_color=None)),
            ('cloudy', dict(line_color='grey', fill_color=None))]:
        sub = ndf[ndf['status'] == status]
        src = ColumnDataSource(sub.assign(lower=sub['zp'] - sub['zp_err'],
                                          upper=sub['zp'] + sub['zp_err']))
        r = p.scatter('night', 'zp', source=src, size=7,
                      legend_label=f'Night: {status}', **kw)
        night_renderers.append(r)
        if status == 'clear':
            w = Whisker(source=src, base='night', lower='lower',
                        upper='upper', line_color='navy', line_alpha=0.6)
            p.add_layout(w)
            # Hide the whiskers along with the points
            r.js_on_change('visible', CustomJS(
                args=dict(w=w), code="w.visible = cb_obj.visible"))
    if (ndf['status'] == 'cloudy').any():
        night_renderers[2].visible = False

    p.add_tools(HoverTool(renderers=night_renderers, tooltips=[
        ('Night', '@night{%F}'), ('ZP', '@zp{0.000} ± @zp_err{0.000}'),
        ('Envelope', '@envelope{0.000}'), ('Status', '@status'),
        ('Median of all', '@zp_median_all{0.000}'),
        ('used / low / high', '@n_used / @n_low / @n_high')],
        formatters={'@night': 'datetime'}))
    p.add_tools(HoverTool(renderers=frame_renderers, tooltips=[
        ('File', '@filename'), ('ZP', '@MCC_ZP{0.000}'),
        ('ZPSD', '@MCC_ZPSD{0.000}'), ('NSAM', '@MCC_NSAM'),
        ('QA', '@qastate')]))

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
    parser.add_argument('--sanity-window', type=int,
                        default=SANITY_WINDOW_DAYS,
                        help='Rolling median window (days) for gross outliers')
    parser.add_argument('--sanity-max-dev', type=float, default=SANITY_MAX_DEV,
                        help='Reject frames further than this (mag) from the '
                             'rolling median')
    parser.add_argument('--floor', type=float, default=0.02,
                        help='Noise floor (mag) added in quadrature to each '
                             'frame error')
    parser.add_argument('--k-lo', type=float, default=2.0,
                        help='Clip frames more than this many sigma below '
                             'the nightly estimate')
    parser.add_argument('--k-hi', type=float, default=3.0,
                        help='Clip frames more than this many sigma above '
                             'the nightly estimate')
    parser.add_argument('--start-pct', type=float, default=80,
                        help='Percentile to start the nightly iteration from')
    parser.add_argument('--env-window', type=int, default=60,
                        help='Cross-night envelope window (days)')
    parser.add_argument('--env-pct', type=float, default=80,
                        help='Percentile of nightly values used as envelope')
    parser.add_argument('--cloud-threshold', type=float, default=0.1,
                        help='Flag nights more than this (mag) below the '
                             'envelope as cloudy')
    parser.add_argument('--outdir', default='.',
                        help='Directory to write HTML plots to')
    parser.add_argument('--cache-dir', default=None,
                        help='Cache fetched tables here and re-use them')
    parser.add_argument('--show', action='store_true',
                        help='Open each plot in a browser')
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    for instrument in args.instruments:
        ptag = args.processing_tag.format(instrument=instrument)
        for filt in args.filters:
            print(f"{instrument} {filt}:")
            url = build_url(args.server, instrument, ptag, filt)
            df = fetch_table(url, cache_dir=args.cache_dir)
            print(f"    Fetched {len(df)} rows")
            if df.empty:
                continue
            df, sanity_rej = filter_rows(df, min_nsam=args.min_nsam,
                                         sanity_window=args.sanity_window,
                                         sanity_max_dev=args.sanity_max_dev)
            if df.empty:
                print("    No rows left after filtering")
                continue
            df = add_night(df, instrument)
            sanity_rej = add_night(sanity_rej, instrument)
            ndf = combine_nights(df, floor=args.floor, k_lo=args.k_lo,
                                 k_hi=args.k_hi, start_pct=args.start_pct)
            ndf = flag_nights(ndf, window=args.env_window, pct=args.env_pct,
                              threshold=args.cloud_threshold)
            print(f"    {len(df)} rows -> {len(ndf)} nights; "
                  f"{int(df['clipped_low'].sum())} rows clipped low, "
                  f"{int(df['clipped_high'].sum())} clipped high")
            print("    Nights: " + ", ".join(
                f"{n} {k}" for k, n in ndf['status'].value_counts().items()))

            p = make_plot(instrument, filt, df, ndf, sanity_rej)
            outfile = os.path.join(args.outdir,
                                   f"gmos_zp_{instrument}_{filt}.html")
            output_file(outfile, title=f"{instrument} {filt} zeropoint")
            if args.show:
                show(p)
            else:
                save(p)
            print(f"    Wrote {outfile}")


if __name__ == '__main__':
    main()
