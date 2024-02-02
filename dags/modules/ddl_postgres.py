from sqlalchemy import create_engine, Column, Integer, SmallInteger, String, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class DimProvince(Base):
    __tablename__ = 'dim_province'
    province_id = Column(SmallInteger, primary_key=True)
    province_name = Column(String(50), nullable=False)

class DimDistrict(Base):
    __tablename__ = 'dim_district'
    district_id = Column(SmallInteger, primary_key=True)
    district_name = Column(String(50), nullable=False)
    province_id = Column(SmallInteger, ForeignKey('dim_province.province_id'))

class DimCase(Base):
    __tablename__ = 'dim_case'
    id = Column(Integer, primary_key=True, autoincrement=True)
    status_name = Column(String(50), nullable=False)
    status_detail = Column(String(50))

class ProvinceDaily(Base):
    __tablename__ = 'province_daily'
    id = Column(Integer, primary_key=True, autoincrement=True)
    province_id = Column(SmallInteger, ForeignKey('dim_province.province_id'))
    case_id = Column(SmallInteger, ForeignKey('dim_case.id'))
    date = Column(Date, nullable=False)
    total = Column(Integer, nullable=False)

class ProvinceMonthly(Base):
    __tablename__ = 'province_monthly'
    id = Column(Integer, primary_key=True, autoincrement=True)
    province_id = Column(SmallInteger, ForeignKey('dim_province.province_id'))
    case_id = Column(SmallInteger, ForeignKey('dim_case.id'))
    month = Column(String(8), nullable=False)
    total = Column(Integer, nullable=False)

class ProvinceYearly(Base):
    __tablename__ = 'province_yearly'
    id = Column(Integer, primary_key=True, autoincrement=True)
    province_id = Column(SmallInteger, ForeignKey('dim_province.province_id'))
    case_id = Column(SmallInteger, ForeignKey('dim_case.id'))
    year = Column(String(5), nullable=False)
    total = Column(Integer, nullable=False)

class DistrictMonthly(Base):
    __tablename__ = 'district_monthly'
    id = Column(Integer, primary_key=True, autoincrement=True)
    district_id = Column(SmallInteger, ForeignKey('dim_district.district_id'))
    case_id = Column(SmallInteger, ForeignKey('dim_case.id'))
    month = Column(String(8), nullable=False)
    total = Column(Integer, nullable=False)

class DistrictYearly(Base):
    __tablename__ = 'district_yearly'
    id = Column(Integer, primary_key=True, autoincrement=True)
    district_id = Column(SmallInteger, ForeignKey('dim_district.district_id'))
    case_id = Column(SmallInteger, ForeignKey('dim_case.id'))
    year = Column(String(5), nullable=False)
    total = Column(Integer, nullable=False)

def ddl_postgres():
    engine = create_engine('postgresql+psycopg2://airflow:airflow@host.docker.internal:5432/covid')

    Base.metadata.create_all(engine)

    print('Table in Postgres successfully created using SQLAlchemy')

if __name__ == "__main__":
    ddl_postgres()
