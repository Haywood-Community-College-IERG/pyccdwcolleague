import collections.abc
import pandas as pd
import re
import sqlalchemy
import urllib
import yaml

__version__ = "0.0.1"

class ColleagueError(Exception):
    pass

class ColleagueConfigurationError(Exception):
    pass

class ColleagueConnection(object):
    """
    Connection to CCDW Data Warehouse built from Colleague data extraction.
    """

    config_schema_history = "history"

    def __init__(self, config=None):
        '''
        The constructor for ColleagueConnection class.

        Parameters:
            config (dictionary): Default=None. Pass in the config file or the constructor will load the 
                                 config file itself. 
        '''
        if config:
            self.__config__ = config.copy()
        else:
            with open("config.yml","r") as ymlfile:
                cfg_l = yaml.load(ymlfile, Loader=yaml.FullLoader)

                if cfg_l["config"]["location"] == "self":
                    self.__config__ = cfg_l.copy()
                else:
                    with open(cfg_l["config"]["location"] + "config.yml","r") as ymlfile2:
                        self.__config__ = yaml.load(ymlfile2, Loader=yaml.FullLoader)

        self.config_schema_history = self.__config__["sql"]["schema_history"]

        self.__conn_details__ = urllib.parse.quote_plus(
            f"DRIVER={{{self.__config__['sql']['driver']}}};"
            f"SERVER={self.__config__['sql']['server']};"
            f"DATABASE={self.__config__['sql']['db']};"
            #f"SCHEMA={__config__['sql']['schema']};"
            f"Trusted_Connection=Yes;"
            f"Description=Python ColleagueConnection Class")

        self.__engine__ = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % self.__conn_details__)

    def get_data(self, file,  cols=[], where="", sep='.', schema="history", version="current", debug="" ):
        '''
        Get data from Colleague data warehouse. 
        
        Parameters:
            file (str): The base name of the Colleague file.
            cols (list or dict): The list of columns to return from the specified file. You can specify
                                 new column names by using a dictionary. 
            where (str): All filters to be applied to the resulting table. These will be sent directly 
                         to SQL Server, but only basic Python filtering using AND, OR, ==, != are allowed.
                         When specifying conditions, do not include square brackets ([]) around the
                         column names. All where conditions are applied before the columns are renamed.
            sep (str): Default=".". Specify the separator value for column names. Colleague names are
                       separated by '.'. Specifying a value here would replace that value with that 
                       character.
            version (str): Default="current". Which version of the data to get. Options are
                           "current" (default), "history", or "all". Option "current" sets the 
                           adds "CurrentFlag='Y'" to the where argument. The other two are 
                           treated the same. This argument is ignored for non-SQL Server-based objects.
            schema (str): Default="history". The schema from which to get the data. This argument is
                          ignored for non-SQL Server-based objects.
            debug (str): Default="". Specify the debug level. Valid debug levels:
                         query: print out the generated query
        '''
        #def getColleagueData( engine, file, cols=[], schema="history", version="latest", where="", sep='.', debug="" ):

        if isinstance(cols,collections.abc.Mapping):
            qry_cols = '*' if cols == [] else ', '.join([f"[{c}] AS [{cols[c]}]" for c in cols])
        else:
            qry_cols = '*' if cols == [] else ', '.join([f"[{c}]" for c in cols])

        if where != "":
            qry_where_base = where

            # Convert VAR != ['ITEM1','ITEM2'] into VAR IN ('ITEM1','ITEM2')
            for f in re.findall(r"!= \[([^]]+)\]",qry_where_base):
                qry_where_base = qry_where_base.replace(f"== [{f}]",f"NOT IN ({f})")
            # Convert VAR == ['ITEM1','ITEM2'] into VAR IN ('ITEM1','ITEM2')
            for f in re.findall(r"== \[([^]]+)\]",qry_where_base):
                qry_where_base = qry_where_base.replace(f"== [{f}]",f"IN ({f})")
            # Convert VAR.NAME into [VAR.NAME]
            for f in re.findall(r"([a-zA-Z]\w*\.[\w\.]+)",qry_where_base):
                qry_where_base = qry_where_base.replace(f,f"[{f}]")
            # Convert remaining == to =
            qry_where_base = qry_where_base.replace("==",'=')
            # Convert remaining == to =
            qry_where_base = qry_where_base.replace("!=",'<>')

            qry_where = "" if where == "" else f"WHERE {qry_where_base}"
        else:
            qry_where = ""

        if (version == "current" and schema==self.config_schema_history):
            qry_where = "WHERE " if where == "" else qry_where + " AND "
            qry_where += f"CurrentFlag='Y'"
#        qry_where = "WHERE CurrentFlag='Y'" if (version == "latest" and schema==self.config_schema_history) else ""
                    
        qry = f"SELECT {qry_cols} FROM {schema}.{file} {qry_where}"

        if debug == "query":
            print(qry)
        
        df = pd.read_sql(qry, self.__engine__)

        if (sep != '.'):
            df.columns = df.columns.str.replace(".", sep)

        return(df)

    def School_ID(self):
        """Return the school's InstID from the config file."""
        return(self.__config__["school"]["instid"])

    def School_IPEDS(self):
        """Return the school's IPEDS ID from the config file."""
        return(self.__config__["school"]["ipeds"])
