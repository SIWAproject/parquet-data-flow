from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, VARCHAR, Float, BIGINT, Integer, String
import sqlalchemy as sa 


Base = declarative_base()
base = declarative_base()

class Project(Base):
    __tablename__ = 'projects'
    projectId = Column(String, primary_key=True)
    animalType = Column(String)
    geneticLine = Column(String)
    client = Column(String)
    externalClient = Column(String)
    country = Column(String)
    samplingDate = Column(String)


class KitProduction(base):
    __tablename__ = 'kits_prod'
    __table_args__ = {'extend_existing': True}
    kitId = sa.Column(String, unique=True, primary_key=True)
    projectId = sa.Column(String)
    age = sa.Column(String)
    treatment = sa.Column(String)
    treatmentNumber = sa.Column(Integer)
    farm = sa.Column(String)
    farmLocation = sa.Column(String)

class KitPets(base):
    __tablename__ = 'kits_pets'
    __table_args__ = {'extend_existing': True}
    kitId = sa.Column(String, unique=True, primary_key=True)
    projectId = sa.Column(String)
    groupingVar = sa.Column(String)
    value = sa.Column(String)


# Definición de la clase AnimalProd adaptada al nuevo esquema
class AnimalProd(base):
    __tablename__ = 'animals_prod'
    __table_args__ = {'extend_existing': True}
    animalId = sa.Column(String, unique=True, primary_key=True)
    identifier = sa.Column(String)
    kitId = sa.Column(String)
    sex = sa.Column(String)
    house = sa.Column(String)
    pen = sa.Column(Integer)
    panels = sa.Column(String)

class AnimalPets(base):
    __tablename__ = 'animals_pets'
    __table_args__ = {'extend_existing': True}
    animalSampleId = sa.Column(String, unique=True, primary_key=True)
    animalId = sa.Column(String)
    kitId = sa.Column(String)
    name = sa.Column(String)
    locationCity = sa.Column(String)
    breed = sa.Column(String)
    species = sa.Column(String)
    ageYears = sa.Column(Float)
    sampleTime = sa.Column(Integer)
    sex = sa.Column(String)


class Microbiome(base):
    __tablename__ = 'microbiome'
    __table_args__ = {'extend_existing': True}
    sampleId = sa.Column(String, unique=True, primary_key=True)
    animalId = sa.Column(String)
    runId = sa.Column(String)
    sampleLocation = sa.Column(String)
    alphaShannon = sa.Column(Float)
    alphaObserved = sa.Column(Float)


class Histopathology(Base):
    __tablename__ = 'histo'
    sampleId = sa.Column(sa.String, primary_key=True, nullable=False)
    score = sa.Column(sa.String)
    animalId = sa.Column(sa.String)
    sampleLocation = sa.Column(sa.String)
    researchNumber = sa.Column(sa.String)
    value = sa.Column(sa.Float)
    uniqueKey = sa.Column(sa.String)

    def asdict(self):
        return {
            'Sample ID': self.sampleId,
            'Score': self.score,
            'Animal ID': self.animalId,
            'Sample Location': self.sampleLocation,
            'Research Number': self.researchNumber,
            'Value': self.value,
            'Unique Key': self.uniqueKey
        }
    

class GeneExpression(base):
    __tablename__ = 'geneexpression'
    sampleId = sa.Column(String)
    plateCode = sa.Column(String)
    animalId = sa.Column(String)
    sampleLocation = sa.Column(String)
    targetGene = sa.Column(String)
    deltaCq = sa.Column(Float)
    uniqueKey = sa.Column(String, primary_key=True, nullable=False)


class OtuCount(Base):
    __tablename__ = 'otucount'
    sampleId = Column(String, primary_key=True)
    otu = Column(String)
    value = Column(Float)
    uniqueKey = Column(String, unique=True, index=True)


class Taxonomy(Base):
    __tablename__ = 'taxonomy'
    otu = Column(String, unique=True, primary_key=True)
    species = Column(String)
    genus = Column(String)
    family = Column(String)
    order = Column(String)
    tclass = Column(String)
    phylum = Column(String)
    kingdom = Column(String)



