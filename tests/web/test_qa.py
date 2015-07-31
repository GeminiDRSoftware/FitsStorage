import pytest
from fits_storage.web import qastuff as wqa
from fits_storage.orm import qastuff as qa
from data import qa_samples
import json

metric_types   = ('iq', 'zp', 'sb', 'pe')
metric_classes = (qa.QAmetricIQ, qa.QAmetricZP, qa.QAmetricSB, qa.QAmetricPE)

@pytest.mark.usefixtures("rollback")
@pytest.mark.parametrize("input,expected", qa_samples.samples)
def test_ingest_json(session, input, expected):
    session.rollback()
    assert wqa.qareport_ingest(json.loads(input), submit_host='localhost', submit_time=qa_samples.st) == 200
    reports = list(session.query(qa.QAreport))
    assert len(reports) == 1
    assert dict(reports[0]) == expected['rep']
    for key, cls in zip(metric_types, metric_classes):
        sample_metrics = expected[key]
        metrics = list(session.query(cls))
        assert sorted(sample_metrics) == sorted(dict(metric) for metric in metrics)
        for metric in metrics:
            session.delete(metric)

    session.delete(reports[0])
    session.commit()
