from pydantic_settings import BaseSettings

class GIS_config(BaseSettings):    
    GIS_USERNAME: str = ""
    GIS_PASSWORD: str = ""
    SERVER: str = 'arthurhouftps.pps.eosdis.nasa.gov'

gis_config = GIS_config()

class Earthdata_config(BaseSettings):
    EARTHDATA_USERNAME: str = ""
    EARTHDATA_PASSWORD: str = ""

earthdata_config = Earthdata_config()

class AOD_config(BaseSettings):
    FTP_USER: str = ""
    FTP_PASS: str = ""

aod_config = AOD_config()

class MODIS_config(BaseSettings):
    TOKEN: str = ""

modis_config = MODIS_config()
