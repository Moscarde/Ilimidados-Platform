import os

from db.database import engine
from etl.load import load_data
from etl.transform import transform_data
from models.base import Base

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
def main():
    # Criar tabelas no banco de dados
    print("Criando tabelas no banco de dados")
    Base.metadata.create_all(bind=engine)

    extraction_dir = r"pipelines/instagram/etl/extraction_files"
    trasnformed_data = transform_data(extraction_dir)
    load_data(trasnformed_data)

if __name__ == "__main__":
    main()