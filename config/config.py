from pydantic_settings import BaseSettings

class GIS_config(BaseSettings):    
    GIS_USERNAME: str = "tr.hoanganh1124work@gmail.com"
    GIS_PASSWORD: str = "tr.hoanganh1124work@gmail.com"
    SERVER: str = 'https://arthurhouhttps.pps.eosdis.nasa.gov/text'

gis_config = GIS_config()

class Earthdata_config(BaseSettings):
    EARTHDATA_USERNAME: str = "sfatew"
    EARTHDATA_PASSWORD: str = "Hoanganh1124work@"

earthdata_config = Earthdata_config()