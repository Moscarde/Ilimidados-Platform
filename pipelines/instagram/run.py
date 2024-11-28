# TODO: desenvolver processo principal que executa to_raw e to_clean

from db.database import engine
from models.base import Base
from models import (
    InstagramAudienceInstagramFollowersRaw,
    InstagramFollowsInstagramFollowsRaw,
    InstagramAudienceDemographicsRaw,
    InstagramAudienceTopCitiesRaw,
    InstagramAudienceTopCountriesRaw,
    InstagramAudienceTopPagesRaw,
    InstagramReachReachRaw,
    InstagramTopContentFormatsPublishedContentRaw,
    InstagramVisitsProfileVisitsRaw,
    InstagramContentPostDataRaw,
    InstagramContentStoryDataRaw)

# Criar tabelas no banco de dados
Base.metadata.create_all(bind=engine)