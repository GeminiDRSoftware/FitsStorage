from fits_storage.db.createtables import GrantHelper

def test_granthelper():
    a = GrantHelper()
    assert a.select_string == ''
    assert a.insert_string == ''
    assert a.update_string == ''
    assert a.delete_string == ''

    a.select('foos')
    a.insert('fooi')
    a.update('foou')
    a.delete('food')
    assert a.select_string == 'foos'
    assert a.insert_string == 'fooi'
    assert a.delete_string == 'food'
    assert a.update_string == 'foou, fooi_id_seq'

    a.select(['sss1', 'sss2'])
    assert a.select_string == 'foos, sss1, sss2'

    a.insert(['i1', 'i2'])
    a.update(['u1', 'u2'])
    assert a.insert_string == 'fooi, i1, i2'
    assert a.update_string == 'foou, u1, u2, fooi_id_seq, i1_id_seq, i2_id_seq'
