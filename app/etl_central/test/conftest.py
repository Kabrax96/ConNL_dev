# tests/conftest.py
from dotenv import load_dotenv
load_dotenv()




# run all tests in your folder
#python -m pytest -q app/etl_central/test

# run a single file
#python -m pytest -q app/etl_central/test/test_s3.py  --check

#python -m pytest -q app/etl_central/test/test_db_conn.py --check

#python -m pytest -q app/etl_central/test/test_load_cp.py -- check
#python -m pytest -q app/etl_central/test/test_transform_cp.py -- not pass
