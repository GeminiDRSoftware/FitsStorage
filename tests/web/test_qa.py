import pytest

from fits_storage.utils.web import adapter
from fits_storage.web import qastuff as wqa
from fits_storage.orm import qastuff as qa
from data import qa_samples
import json

metric_types   = ('iq', 'zp', 'sb', 'pe')
metric_classes = (qa.QAmetricIQ, qa.QAmetricZP, qa.QAmetricSB, qa.QAmetricPE)

# Initialize Adapter Context
_context = adapter.get_context(True)


@pytest.mark.usefixtures("rollback")
@pytest.mark.parametrize("input,expected", qa_samples.samples)
@pytest.mark.slow
def test_ingest_json(session, input, expected):
    # hack
    _context.session = session

    session.rollback()
    #O assert wqa.qareport_ingest(json.loads(input), submit_host='localhost', submit_time=qa_samples.st) == 200
    # No return code for method, dropping assert
    wqa.qareport_ingest(json.loads(input), submit_host='localhost', submit_time=qa_samples.st)
    reports = list(session.query(qa.QAreport))
    assert len(reports) == 1
    assert dict(reports[0]) == expected['rep']
    for key, cls in zip(metric_types, metric_classes):
        sample_metrics = expected[key]
        metrics = list(session.query(cls))
        dmetrics = [dict(metric) for metric in metrics]
        assert sorted(sample_metrics, key=lambda i: i['filename']) == sorted(dmetrics, key=lambda i: i['filename'])
        for metric in metrics:
            session.delete(metric)

    session.delete(reports[0])
    session.commit()
