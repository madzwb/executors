import sys



CONFIG = "config"
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
