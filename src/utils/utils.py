import os 
import json 
import sys
from functools import wraps
import time
from jinja2 import Template

def render_sql (sql_str, params):
    """ Renders dynamic sql query using Jinja template 
    Args:
        sql_str (String) : Sql string
        params (Dict) : Input parameters used by given sql query
    Returns: renderd sql query as string
    """
    template = Template(sql_str)
    sql_query = template.render(params)
    print(sql_query)
    return sql_query


def timing(f):
    @wraps(f)
    def wrap(*args,**kw):
        start_time = time.time()
        result = f(*args,**kw)
        end_time = time.time()
        total_time = end_time - start_time
        print(f"TOTAL TIME TAKEN FOR FUNCTION {f.__name__} IS {total_time}")
