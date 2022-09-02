from sqlalchemy import create_engine, Column,ForeignKey,Integer,DECIMAL,String,BIGINT
from sqlalchemy.orm import sessionmaker,declarative_base,relationship
from sqlalchemy_utils  import database_exists,create_database

def conecciondb(dbms, user, password, host, port, db_name):
    # -- Creación del engine y de la db -- #
    engine = create_engine(f'{dbms}://{user}:{password}@{host}:{port}/{db_name}')
    if not database_exists(engine.url):
        create_database(engine.url)

    Base = declarative_base()  # Usada para declarar y crear las tablas

    #crear tablas 

    class Year(Base):
        __tablename__='year'
        id_year=Column(Integer, primary_key=True)
        year=Column(Integer)

    class Country(Base):
        __table_name='country'
        id_country=Column(Integer,primary_key=True)
        country_name=Column(String(100))
        country_code=Column(String(10))
    
    class Energy(Base):    
        __tablename__='energy'
        id_energy=Column(Integer,primary_key=True)
        energy_name=Column(String(50))
    
    class Irradiation(Base) :
        __tablename__='irradiation'
        id_irradation=Column(Integer,primary_key=True)
        id_country=relationship('country')
        id_year=relationship('year')
        annual_irradiation=Column(DECIMAL(10,2))

    class Population(Base):
        __tablename__='population'
        id_population=Column(Integer,primary_key=True)
        id_country=relationship('country')
        id_year=relationship('year')
        annual_population=Column(DECIMAL(15,2))

    class Production(Base):
        __tablename__='production'
        id_production=Column(Integer,primary_key=True)
        id_country=relationship('country')
        id_year=relationship('year')
        annual_production=Column(DECIMAL(15,2))

    class Co2Emission(Base):
            __tablename__='co2_emission'
            id_emission=Column(Integer,primary_key=True)
            id_country=relationship('country')
            id_year=relationship('year')
            annual_emission=Column(DECIMAL(15,2))
    class Consumption(Base):
                __tablename__='consumption'
                id_consumption=Column(Integer,primary_key=True)
                id_country=relationship('country')
                id_year=relationship('year')
                annual_consumption=Column(DECIMAL(15,2))


    Base.metadata.create_all(engine)
    engine.dispose()
