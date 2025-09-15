import os
import pytest

class TestSuccessHelper:


    def test_data_reading_csv(self, env):
        from main.commons.helper import data_reading

        df = data_reading(os.getenv('pays'), False)

        assert 756483 == df['pay_date'].count()

    def test_data_reading_json(self):
        from main.commons.helper import data_reading

        df = data_reading(os.getenv('prints'), True)

        assert 508617 == df['day'].count()


class TestErrorHelper:
    def test_data_reading_json(self):
        from main.commons.helper import data_reading

        with pytest.raises(ValueError):
            data_reading(os.getenv('fail'), True)