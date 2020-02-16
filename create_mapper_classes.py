import numpy as np
import pandas as pd
import pathlib

def panda_to_sql_types_mapper(value_to_map):
    mapper = {
        "float" : "NUMERIC",
        "int":"INTEGER",
        "object":"VARCHAR"
    }

    for key in mapper:
        if key in value_to_map.name:
            return mapper.get(key)
    
    raise NameError('No mapping found this type')

#specify data folder
data_path = pathlib.Path('data')

#specify pattern to look in data_path, we use wildcard.
#data_path.glob('*') return a generator
files = data_path.glob('*')

mapping_output_dir = "mapping_classes"
if not pathlib.Path(mapping_output_dir).exists():
    pathlib.Path(mapping_output_dir).mkdir()

for file_path in files:

    df = pd.read_csv(file_path,nrows=100_000,keep_default_na=False)
    df = df.apply(pd.to_numeric, errors='ignore',downcast='integer')
    # schema = pd.io.json.build_table_schema(df)      
    custom_schema_for_sql = df.dtypes.to_dict().items() 

    core_class_list = []
    for key,value in custom_schema_for_sql:
            
        core_class_list.append("""{0} = Column({1})""".format(key,panda_to_sql_types_mapper(value)))

    #class generation
    class_name = file_path.stem

    item_types = set([panda_to_sql_types_mapper(_type) for _type in set(map(lambda item: item[1],custom_schema_for_sql))])

    item_types = ",".join(item_types)

    list_header = ["import sqlalchemy",f"from sqlalchemy import Column,{item_types}",f"from {mapping_output_dir} import Base"]
            
    class_import_header = "\n".join(list_header)

    class_body = ["\nclass {0}(Base):".format(class_name),"__tablename__ = '{0}'".format(class_name)]

    class_body =  class_body + core_class_list
            
    class_body = "\n\t".join(class_body)
                
    class_generated =f"""{class_import_header}
        {class_body}
        """
    #write generated classes for mapping, generated classes need to be however changed for specifying Primary Keys and more
    with open(f'{mapping_output_dir}\\class_{class_name}.py','w') as output_class:
        output_class.writelines(class_generated)

    print(f"Class {class_name} generated")

print("Base file generated")
base_def = ["from sqlalchemy.ext.declarative import declarative_base","Base = declarative_base()"]
base_def = "\n".join(base_def)

#write __init__.py
with open(f'{mapping_output_dir}\\__init__.py','w') as output_class:
        output_class.writelines(base_def)