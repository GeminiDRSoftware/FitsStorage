from fits_storage.orm.notification import Notification


def test_notifiction():
    notification = Notification("label")
    assert(notification.label == "label")
