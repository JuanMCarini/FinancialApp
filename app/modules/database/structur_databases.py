from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, DECIMAL, ForeignKey, Numeric, Enum
from sqlalchemy.orm import relationship
from sqlalchemy import Enum


Base = declarative_base()

class Province(Base):
    __tablename__ = 'provinces'
    
    ID = Column(Integer, primary_key=True, autoincrement=True)
    Name = Column(String(100), nullable=False)

    def __repr__(self):
        return f"<Province(ID={self.ID}, Name='{self.Name}')>"

class MaritalStatus(Enum):
    SINGLE = 'SINGLE'
    COHABITATION = 'COHABITATION'
    MARRIED = 'MARRIED'
    WIDOW = 'WIDOW'
    DIVORCE = 'DIVORCE'

class Customer(Base):
    __tablename__ = 'customers'
    
    ID = Column(Integer, primary_key=True, autoincrement=True)
    CUIL = Column(BigInteger, nullable=False)
    DNI = Column(BigInteger, nullable=False)
    Last_Name = Column(String(100), nullable=False)
    Name = Column(String(100), nullable=False)
    Gender = Column(String(2), nullable=False, default='O')
    Date_Birth = Column(DateTime)
    Marital_Status =  Column(MaritalStatus)
    Age_at_Discharge = Column(Integer)
    Country = Column(String(100), nullable=False, default='Argentina')
    ID_Province = Column(Integer, ForeignKey('provinces.ID', ondelete='SET NULL', onupdate='CASCADE'))
    Locality = Column(String(100))
    Street = Column(String(100))
    Nro = Column(Integer)
    CP = Column(Integer)
    Feature = Column(Integer)
    Telephone = Column(BigInteger)
    Seniority = Column(Integer)
    Salary = Column(DECIMAL(20, 2))
    CBU = Column(String(22))
    Collection_Entity = Column(String(100))
    Employer = Column(String(100))
    Dependence = Column(String(100))
    CUIT_Employer = Column(String(22))
    ID_Empl_Prov = Column(Integer, ForeignKey('provinces.ID', ondelete='SET NULL', onupdate='CASCADE'))
    Empl_Loc = Column(String(100))
    Empl_Adress = Column(String(100))
    Last_Update = Column(DateTime)

    # Relationships
    province = relationship("Province", foreign_keys=[ID_Province])
    employer_province = relationship("Province", foreign_keys=[ID_Empl_Prov])

class Company(Base):
    __tablename__ = 'companies'

    ID = Column(Integer, primary_key=True, autoincrement=True)
    Social_Reason = Column(String(100), nullable=False)
    CUIT = Column(BigInteger, nullable=False)
    Advance = Column(Numeric(15, 2), nullable=False, default=0)

    def __repr__(self):
        return f"<Company(ID={self.ID}, Social_Reason='{self.Social_Reason}', CUIT={self.CUIT}, Advance={self.Advance})>"
    
class Installments(Base):
    __tablename__ = 'installments'

    id = Column("ID", Integer, primary_key=True, autoincrement=True)
    id_op = Column("ID_Op", Integer, ForeignKey("credits.ID", onupdate="CASCADE"), nullable=False)
    nro_inst = Column("Nro_Inst", Integer, nullable=False)
    d_due = Column("D_Due", DateTime, nullable=False)
    capital = Column("Capital", DECIMAL(15, 2), nullable=False)
    interest = Column("Interest", DECIMAL(15, 2), nullable=False)
    iva = Column("IVA", DECIMAL(15, 2), nullable=False)
    total = Column("Total", DECIMAL(15, 2), nullable=False)
    id_owner = Column("ID_Owner", Integer, ForeignKey("companies.ID", onupdate="CASCADE"))
    
    # Relationship with Collection (optional)
    collections = relationship("Collection", back_populates="installment")

class Collection(Base):
    __tablename__ = 'collection'

    id = Column("ID", Integer, primary_key=True, autoincrement=True)
    id_inst = Column("ID_Inst", Integer, ForeignKey("installments.ID", onupdate="CASCADE"))
    d_emission = Column("D_Emission", DateTime, nullable=False)
    type_collection = Column("Type_Collection", Enum('COMUN', 'ANTICIPADA', 'PARCIAL', 'REDONDEO', 
                                                     'PENALTY', 'CAN. ANT.', 'BON. CAN. ANT.', 
                                                     'REVERSA', 'NO COMPRADA', 'RECURSO'), nullable=True)
    capital = Column("Capital", DECIMAL(15, 2), nullable=False)
    interest = Column("Interest", DECIMAL(15, 2), nullable=False)
    iva = Column("IVA", DECIMAL(15, 2), nullable=False)
    total = Column("Total", DECIMAL(15, 2), nullable=False)

    # Relationship with Installments
    installment = relationship("Installments", back_populates="collections")
