from pydantic_settings import BaseSettings

class GIS_config(BaseSettings):    
    GIS_USERNAME: str = ""
    GIS_PASSWORD: str = ""
    SERVER: str = 'https://arthurhouhttps.pps.eosdis.nasa.gov/text'

gis_config = GIS_config()

class Earthdata_config(BaseSettings):
    EARTHDATA_USERNAME: str = ""
    EARTHDATA_PASSWORD: str = ""

earthdata_config = Earthdata_config()

class AOD_config(BaseSettings):
    FTP_USER = ""
    FTP_PASS = ""