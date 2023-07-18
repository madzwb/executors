import sys



CONFIG = "config"
if  \
        CONFIG not in sys.modules\
    or  CONFIG not in globals()\
    or  CONFIG not in locals()\
:
    class Config:
        DEBUG = True\
                    if hasattr(sys, "gettrace") and sys.gettrace()\
                    else\
                False
        
    config = Config()
