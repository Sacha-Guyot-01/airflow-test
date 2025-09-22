import re

def read_and_prepare(sql_file, **kwargs):
    content = open(sql_file).read()
    for k in kwargs.keys():
        content = content.replace(f"@{k.upper()}@", kwargs[k])
    return content

def get_params(dag_name, **kwargs):
    root = dag_name.split("_")[0]
    conf = open(f"../conf/{root.upper()}/{dag_name.upper()}.yaml").read()
    project = conf["project"]
    location = conf["location"]
    sa_key = None # get SA key from secret manager
    params = {"DAG":dag_name, "PROJECT":project, "LOCATION":location, **kwargs}
    return [f"../sql/{root.upper()}/{dag_name.upper()}/", f"../conf/{root.upper()}/{dag_name.upper()}.yaml", sa_key, params]