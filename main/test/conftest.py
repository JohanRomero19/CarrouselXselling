import pytest
import os

@pytest.fixture(scope='session')
def env():
    os.environ['pays'] = "../landing/pays.csv"
    os.environ['prints'] = "../landing/prints.json"
    os.environ['taps'] = "../landing/taps.json"
    os.environ['fail'] = "../landing/taps.parquet"