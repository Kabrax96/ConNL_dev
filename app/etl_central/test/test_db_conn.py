import os, pytest
from sqlalchemy import create_engine, text

@pytest.mark.order(1)
def test_db_connect_and_permissions():
    dsn = f"postgresql+psycopg2://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('SERVER_NAME')}:{os.getenv('PORT')}/{os.getenv('DATABASE_NAME')}"
    engine = create_engine(dsn)
    with engine.begin() as con:
        con.execute(text("create table if not exists _conn_probe (id int primary key)"))
        con.execute(text("insert into _conn_probe (id) values (1) on conflict (id) do nothing"))
        val = con.execute(text("select count(*) from _conn_probe")).scalar_one()
    assert val >= 1
