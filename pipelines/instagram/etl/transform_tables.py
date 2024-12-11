from datetime import datetime

import pandas as pd

EXTRACTION_DATE = datetime.now().date()


def apply_transformations(tables: dict):
    """
    Applies specific transformations to each table.

    Parameters:
        tables (dict): Dictionary with table names as keys and DataFrames as values.

    Returns:
        dict: Dictionary with transformed tables.
    """

    transformed_tables = {}
    for table_name, df in tables.items():
        if table_name in TRANSFORMATIONS:
            new_table_name, df = TRANSFORMATIONS[table_name](df)
            transformed_tables[new_table_name] = df
        else:
            print(f"Transformação não encontrada para a tabela: {table_name}")
    return transformed_tables


def transform_instagram_visits_profile_visits_raw(df):
    """
    Transforms the Instagram profile visits table to the format suitable for the ORM model.

    Parameters:
        df (pd.DataFrame): DataFrame containing 'Data' and 'Primary' columns.

    Returns:
        tuple: The table name and the transformed DataFrame.
    """

    df = df.rename(columns={"Data": "date", "Primary": "primary_visits"})

    df["date"] = pd.to_datetime(df["date"]).dt.date

    df["extraction_date"] = EXTRACTION_DATE

    new_table_name = "instagram_visits_profile_visits_raw"

    return new_table_name, df


def transform_instagram_reach_reach_raw(df):
    """
    Transforms the Instagram reach table to the format suitable for the ORM model.

    Parameters:
        df (pd.DataFrame): DataFrame containing 'Data' and 'Primary' columns.

    Returns:
        tuple: The table name and the transformed DataFrame.
    """

    df = df.rename(columns={"Data": "date", "Primary": "primary_reach"})

    df["date"] = pd.to_datetime(df["date"]).dt.date

    df["extraction_date"] = EXTRACTION_DATE

    new_table_name = "instagram_reach_reach_raw"

    return new_table_name, df


def transform_instagram_follows_instagram_follows_raw(df):
    """
    Transforms the Instagram followers table to the format suitable for the ORM model.

    Parameters:
        df (pd.DataFrame): DataFrame containing 'Data' and 'Primary' columns.

    Returns:
        tuple: The table name and the transformed DataFrame.
    """

    df = df.rename(columns={"Data": "date", "Primary": "primary_followers"})

    df["date"] = pd.to_datetime(df["date"]).dt.date

    df["extraction_date"] = EXTRACTION_DATE

    new_table_name = "instagram_follows_instagram_follows_raw"

    return new_table_name, df


def transform_instagram_top_content_formats_published_content_raw(df):
    """
    Transforms the Instagram followers table to the format suitable for the ORM model.

    Parameters:
        df (pd.DataFrame): DataFrame containing 'Data' and 'Primary' columns.

    Returns:
        tuple: The table name and the transformed DataFrame.
    """

    df = df.rename(columns={"Stories": "stories", "Publicações": "posts"})

    df["extraction_date"] = EXTRACTION_DATE

    new_table_name = "instagram_top_content_formats_published_content_raw"

    return new_table_name, df


def transform_instagram_content_post_data_raw(df):
    """
    Transforms the Instagram content post data table to the format suitable for the ORM model.

    Parameters:
        df (pd.DataFrame): DataFrame containing content post data columns.

    Returns:
        tuple: The table name and the transformed DataFrame.
    """

    df = df.rename(
        columns={
            "Identificação da publicação": "post_id",
            "Identificação da conta": "ig_account_id",
            "Nome de usuário da conta": "ig_account_username",
            "Nome da conta": "ig_account_name",
            "Descrição": "description",
            "Duração (segundos)": "duration_sec",
            "Horário de publicação": "publish_time",
            "Link permanente": "permalink",
            "Tipo de publicação": "post_type",
            "Comentário de dados": "data_comment",
            "Data": "date",
            "Impressões": "impressions",
            "Alcance": "reach",
            "Curtidas": "likes",
            "Compartilhamentos": "shares",
            "Reproduções": "follows",
            "Comentários": "comments",
            "Salvamentos": "saves",
        }
    )

    df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    data_columns = ["date", "publish_time"]
    df[data_columns] = df[data_columns].applymap(lambda x: None if pd.isna(x) else x)

    numeric_columns = [
        "duration_sec",
        "impressions",
        "reach",
        "likes",
        "shares",
        "follows",
        "comments",
        "saves",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")
        df[column] = df[column].fillna(0)

    text_columns = ["data_comment"]
    for column in text_columns:
        df[column] = df[column].astype(str)

    new_table_name = "instagram_content_post_data_raw"

    return new_table_name, df


def transform_instagram_content_story_data_raw(df):
    """
    Transforms the Instagram content story data table to the format suitable for the ORM model.

    Parameters:
        df (pd.DataFrame): DataFrame containing content story data columns.

    Returns:
        tuple: The table name and the transformed DataFrame.
    """

    df = df.rename(
        columns={
            "Identificação da publicação": "story_id",
            "Identificação da conta": "ig_account_id",
            "Nome de usuário da conta": "ig_account_username",
            "Nome da conta": "ig_account_name",
            "Descrição": "description",
            "Duração (segundos)": "duration_sec",
            "Horário de publicação": "publish_time",
            "Link permanente": "permalink",
            "Tipo de publicação": "post_type",
            "Comentário de dados": "data_comment",
            "Data": "date",
            "Impressões": "impressions",
            "Alcance": "reach",
            "Curtidas": "likes",
            "Compartilhamentos": "shares",
            "Visitas ao perfil": "profile_visits",
            "Respostas": "replies",
            "Toques em figurinhas": "stickers_taps",
            "Navegação": "navigation",
        }
    )

    df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    data_columns = ["date", "publish_time"]
    df[data_columns] = df[data_columns].applymap(lambda x: None if pd.isna(x) else x)

    numeric_columns = [
        "duration_sec",
        "impressions",
        "reach",
        "likes",
        "shares",
        "profile_visits",
        "replies",
        "stickers_taps",
        "navigation",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")
        df[column] = df[column].fillna(0)

    text_columns = ["permalink", "data_comment", "description"]
    for column in text_columns:
        df[column] = df[column].astype(str)

    new_table_name = "instagram_content_story_data_raw"

    return new_table_name, df


def transform_instagram_audience_instagram_followers_raw(df):
    """
    Transforms the Instagram audience followers table to the format suitable for the ORM model.

    Parameters:
        df (pd.DataFrame): DataFrame containing follower data.

    Returns:
        tuple: The table name and the transformed DataFrame.
    """

    df["extraction_date"] = EXTRACTION_DATE

    df = df.rename(columns={"IG_ACCOUNT,FOLLOW,UNIQUE_USERS": "followers"})

    new_table_name = "instagram_audience_instagram_followers_raw"

    return new_table_name, df


def transform_instagram_audience_top_cities_raw(df):
    """
    Transforms the Instagram audience top cities table to the format suitable for the ORM model.

    Parameters:
        df (pd.DataFrame): DataFrame containing audience cities data.

    Returns:
        tuple: The table name and the transformed DataFrame.
    """

    df["extraction_date"] = EXTRACTION_DATE

    df = df.rename(columns={"Principais cidades": "city", "Valor": "value_percentage"})

    df["value_percentage"] = (
        df["value_percentage"].str.replace(",", ".").str.replace("%", "").astype(float)
        / 100
    )

    new_table_name = "instagram_audience_top_cities_raw"

    return new_table_name, df


def transform_instagram_audience_top_countries_raw(df):
    """
    Transforms the Instagram audience top countries table to the format suitable for the ORM model.

    Parameters:
        df (pd.DataFrame): DataFrame containing audience countries data.

    Returns:
        tuple: The table name and the transformed DataFrame.
    """

    df["extraction_date"] = EXTRACTION_DATE

    df = df.rename(
        columns={"Principais países": "country", "Valor": "value_percentage"}
    )

    df["value_percentage"] = (
        df["value_percentage"].str.replace(",", ".").str.replace("%", "").astype(float)
        / 100
    )

    new_table_name = "instagram_audience_top_countries_raw"

    return new_table_name, df


def transform_instagram_audience_demographics_raw(df):
    """
    Transforms the Instagram audience demographics table to the format suitable for the ORM model.

    Parameters:
        df (pd.DataFrame): DataFrame containing audience demographics data.

    Returns:
        tuple: The table name and the transformed DataFrame.
    """

    df["extraction_date"] = EXTRACTION_DATE

    df = df.rename(
        columns={
            "Idade": "age",
            "Mulheres": "women_percentage",
            "Homens": "men_percentage",
        }
    )

    df["women_percentage"] = (
        df["women_percentage"].str.replace(",", ".").str.replace("%", "").astype(float)
        / 100
    )
    df["men_percentage"] = (
        df["men_percentage"].str.replace(",", ".").str.replace("%", "").astype(float)
        / 100
    )

    new_table_name = "instagram_audience_demographics_raw"

    return new_table_name, df


TRANSFORMATIONS = {
    "Visitas_ao_perfil_do_Instagram": transform_instagram_visits_profile_visits_raw,
    "Alcance_do_Instagram": transform_instagram_reach_reach_raw,
    "Seguidores_no_Instagram": transform_instagram_follows_instagram_follows_raw,
    "Conteúdo_publicado": transform_instagram_top_content_formats_published_content_raw,
    "instagram_content_post_data_raw": transform_instagram_content_post_data_raw,
    "instagram_content_story_data_raw": transform_instagram_content_story_data_raw,
    "Seguidores_do_Instagram": transform_instagram_audience_instagram_followers_raw,
    "Seguidores_do_Instagram_pelas_principais_cidades": transform_instagram_audience_top_cities_raw,
    "Seguidores_do_Instagram_pelos_principais_países": transform_instagram_audience_top_countries_raw,
    "Seguidores_do_Instagram_por_gênero_e_idade": transform_instagram_audience_demographics_raw,
}
