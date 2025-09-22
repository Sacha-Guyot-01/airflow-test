import re, os, yaml

def read_and_prepare(sql_file, **kwargs):
    content = open(sql_file).read()
    for k in kwargs.keys():
        content = content.replace(f"@{k.upper()}@", kwargs[k])
    return content

def get_params(dag_name, **kwargs):
    root_path = os.environ["PYTHONPATH"]
    root = dag_name.split("_")[0]
    conf = yaml.load(f"{root_path}/conf/{root.upper()}/{dag_name.upper()}.yaml", Loader=yaml.SafeLoader)
    project = conf["project"]
    location = conf["location"]
    sa_key = None # get SA key from secret manager
    params = {"DAG":dag_name, **kwargs, **conf}
    return [f"{root_path}/sql/{root.upper()}/{dag_name.upper()}/", conf, sa_key, params]