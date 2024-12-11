import pandas as pd
from db.database import engine
from models import (
    InstagramAudienceDemographicsRaw,
    InstagramAudienceInstagramFollowersRaw,
    InstagramAudienceTopCitiesRaw,
    InstagramAudienceTopCountriesRaw,
    InstagramContentPostDataRaw,
    InstagramContentStoryDataRaw,
    InstagramFollowsInstagramFollowsRaw,
    InstagramReachReachRaw,
    InstagramTopContentFormatsPublishedContentRaw,
    InstagramVisitsProfileVisitsRaw,
)
from models.base import Base
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)
session = Session()


df_to_model = {
    "instagram_audience_demographics_raw": InstagramAudienceDemographicsRaw,
    "instagram_audience_instagram_followers_raw": InstagramAudienceInstagramFollowersRaw,
    "instagram_audience_top_cities_raw": InstagramAudienceTopCitiesRaw,
    "instagram_audience_top_countries_raw": InstagramAudienceTopCountriesRaw,
    "instagram_follows_instagram_follows_raw": InstagramFollowsInstagramFollowsRaw,
    "instagram_reach_reach_raw": InstagramReachReachRaw,
    "instagram_top_content_formats_published_content_raw": InstagramTopContentFormatsPublishedContentRaw,
    "instagram_visits_profile_visits_raw": InstagramVisitsProfileVisitsRaw,
    "instagram_content_post_data_raw": InstagramContentPostDataRaw,
    "instagram_content_story_data_raw": InstagramContentStoryDataRaw,
}


def data_exists(model, record):
    """
    Checks if a record already exists in the database for the specified model.

    Parameters:
        model (SQLAlchemy Model): The ORM model to query.
        record (dict): A dictionary representing the record to check for existence.

    Returns:
        bool: True if the record exists, otherwise False.
    """
    return session.query(model).filter_by(**record).first() is not None


def load_and_insert_data(df, model):
    """
    Loads data from a DataFrame and inserts it into the corresponding database table, ignoring duplicate records.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the data to be loaded.
        model (SQLAlchemy Model): The ORM model representing the table to insert data into.

    Returns:
        None: The function handles the insertion and does not return a value.
    """
    records = df.to_dict(orient="records")

    for record in records:
        if not data_exists(model, record):
            try:
                session.bulk_insert_mappings(model, [record])
                session.commit()
                print(f"Dado inserido com sucesso: {record}")
            except Exception as e:
                session.rollback()
                print(f"Erro ao inserir dado: {record}. Erro: {e}")
        else:
            print(f"Dado já existe, ignorado: {record}")


def load_data(transformed_data):
    """
    Manages the process of loading and inserting data for all DataFrames in the `transformed_data` dictionary.

    Parameters:
        transformed_data (dict): A dictionary where keys are table names and values are DataFrames to be loaded.

    Returns:
        None: The function handles the loading of data and does not return a value.
    """

    session = Session()

    try:

        for df_key, df in transformed_data.items():
            model = df_to_model.get(df_key)
            if model is not None and not df.empty:
                print(f"Iniciando injeção de dados para: {df_key}")
                load_and_insert_data(df, model)
            else:
                print(f"DataFrame {df_key} não encontrado ou está vazio. Ignorado.")
    except Exception as e:
        print(f"Erro ao carregar os dados: {e}")
        session.rollback()
    finally:
        session.close()
