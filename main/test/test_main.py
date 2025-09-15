import os
import pytest

class TestSuccessMain:


    def test_data_reading_csv(self, env):
        from main.main import main

        df = main()



# class TestErrorHelper:
#     def test_data_reading_json(self):
#         from main.commons.helper import data_reading

#         with pytest.raises(ValueError):
#             data_reading(os.getenv('fail'), True)