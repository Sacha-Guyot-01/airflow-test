import re, os, yaml

def read_and_prepare(sql_file, params=None):
    content = open(sql_file).read()
    if params:
        for k in params.keys():
            content = content.replace(f"@{k.upper()}@", params[k])
    return content

def get_params(dag_name, **kwargs):
    root_path = os.environ["PYTHONPATH"]
    root = dag_name.split("_")[0]
    conf = yaml.load(open(f"{root_path}/conf/{root.upper()}/{dag_name.upper()}.yaml").read(), Loader=yaml.SafeLoader)
    print(conf)
    project = conf["project"]
    location = conf["location"]
    sa_key = None # get SA key from secret manager
    params = {"DAG":dag_name, **kwargs, **conf}
    return [f"{root_path}/sql/{root.upper()}/{dag_name.upper()}/", conf, sa_key, params]