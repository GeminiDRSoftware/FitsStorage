import pytest

from fits_storage.web.templating import group_digits


@pytest.mark.parametrize('value, decimals, expected',
                         [
                             (12345, 0, '12,345'),
                             (12345, 2, '12,345.00'),
                             (12345.6, 2, '12,345.60'),
                             (12345.678, 2, '12,345.68'),
                             (None, 2, ''),
                         ])
def test_group_digits(value, decimals, expected):
    assert(group_digits(value, decimals=decimals) == expected)


if __name__ == "__main__":
    pytest.main()
