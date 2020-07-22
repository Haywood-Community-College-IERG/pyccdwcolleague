import collections.abc
import pandas as pd
import sqlalchemy
import urllib
import yaml

__version__ = "0.0.0b1"

class ColleagueError(Exception):
    pass

class ColleagueConfigurationError(Exception):
    pass

class ColleagueConnection(object):
    """
    Connection to CCDW Data Warehouse built from Colleague data extraction.
    """

    config_schema_history = "history"

    def __init__(self):
        with open("config.yml","r") as ymlfile:
            cfg_l = yaml.load(ymlfile, Loader=yaml.FullLoader)

            if cfg_l["config"]["location"] == "self":
                self.config = cfg_l.copy()
            else:
                with open(cfg_l["config"]["location"] + "config.yml","r") as ymlfile2:
                    self.config = yaml.load(ymlfile2, Loader=yaml.FullLoader)

        self.config_schema_history = self.config["sql"]["schema_history"]

        self.conn_details = urllib.parse.quote_plus(
            f"DRIVER={{{self.config['sql']['driver']}}};"
            f"SERVER={self.config['sql']['server']};"
            f"DATABASE={self.config['sql']['db']};"
            #f"SCHEMA={config['sql']['schema']};"
            f"Trusted_Connection=Yes;"
            f"Description=Python ColleagueConnection Class")

        self.engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % self.conn_details)

    def get_data(self, file,  cols=[], schema="history", version="latest", where="", sep='.', debug="" ):
        #def getColleagueData( engine, file, cols=[], schema="history", version="latest", where="", sep='.', debug="" ):

        if isinstance(cols,collections.abc.Mapping):
            qry_cols = '*' if cols == [] else ', '.join([f"[{c}] AS [{cols[c]}]" for c in cols])
        else:
            qry_cols = '*' if cols == [] else ', '.join([f"[{c}]" for c in cols])
            
        qry_where = "" if where == "" else f"WHERE {where}"
        
        if (version == "latest" and schema==self.config_schema_history):
            qry_where = "WHERE " if where == "" else qry_where + " AND "
            qry_where += f"CurrentFlag='Y'"
                    
        qry = f"SELECT {qry_cols} FROM {schema}.{file} {qry_where}"

        if debug == "query":
            print(qry)
        
        df = pd.read_sql(qry, self.engine)

        if (sep != '.'):
            df.columns = df.columns.str.replace(".", sep)

        return(df)

    def School_ID(self):
        return(self.config["school"]["instid"])
