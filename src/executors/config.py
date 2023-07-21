import sys


config = None
# if "logger.config" in sys.modules:
try:
    import logger.config as config
    config.LOG_FILE_TRUNCATE = True
    CONFIG = "logger.config"
except ImportError:
    pass

if config is None:
    CONFIG = "config"
    DUMMY = 0


    if  \
            CONFIG not in sys.modules\
        or  CONFIG not in globals()\
        or  CONFIG not in locals()\
    :
        # If module "config" not imported, not defined in environments
        # create instance of dummy class "Config"
        class Config:
            DEBUG = True\
                        if hasattr(sys, "gettrace") and sys.gettrace()\
                        else\
                    False
        config = Config()
