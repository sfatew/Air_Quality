from pydantic_settings import BaseSettings

class GIS_config(BaseSettings):    
    GIS_USERNAME: str = "tr.hoanganh1124work@gmail.com"
    GIS_PASSWORD: str = "tr.hoanganh1124work@gmail.com"
    SERVER: str = 'arthurhouftps.pps.eosdis.nasa.gov'

gis_config = GIS_config()

class Earthdata_config(BaseSettings):
    EARTHDATA_USERNAME: str = "sfatew"
    EARTHDATA_PASSWORD: str = "Hoanganh1124work@"

earthdata_config = Earthdata_config()

class AOD_config(BaseSettings):
    FTP_USER: str = "tr.hoanganh1124work_gmail.com"
    FTP_PASS: str = "SP+wari8"

aod_config = AOD_config()

class MODIS_config(BaseSettings):
    TOKEN: str = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InNmYXRldyIsImV4cCI6MTc3NjAwNTM2NSwiaWF0IjoxNzcwODIxMzY1LCJpc3MiOiJodHRwczovL3Vycy5lYXJ0aGRhdGEubmFzYS5nb3YiLCJpZGVudGl0eV9wcm92aWRlciI6ImVkbF9vcHMiLCJhY3IiOiJlZGwiLCJhc3N1cmFuY2VfbGV2ZWwiOjN9.b7qpd3_fNBxt1sNAKyLdY9P5H4WzDDDwKFrArxXouGwpKezGUfHqaWkE1A1K_59D-aH32NcfcAuKza99PXqtX9riAnQgr7S7VrJiV_jgMNprjZyqhrVWgTvzJcWJFnfphJg_w7clJGOctke6hXN2-kyIlNPHvT4an7ju00R6cYtCQ4s-1ZKPT0JN4flAC9faC651rlNGZhH3GUYlGNNVNV_y1Kmmi_7R2YrxyTTKnK4XKk0Kb5KsJCro1YwgyqqvR1sRxSoXMenWrtMpQPsMXJ2un6AWFTCvbnx9h1nVMsFX4v9NuxKNhAXCD4kv1OlQcE2cC3nQw0RAt1VYtPYdXQ"

modis_config = MODIS_config()