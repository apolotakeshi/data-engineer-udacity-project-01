import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

def choose_template(key):
    """
    - Fetch the template considering the provided key
    - Returns a template to be filled using f-string
    """
    
    template={}
    template["postgres"]     = "host={DATABASE_IP_ADDRESS} dbname={DATABASE_NAME} user={DATABASE_USERNAME} password={DATABASE_PASSWORD}"
    template["postgres_url"] = "{DATABASE_SERVER}+{DATABASE_DRIVER}://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP_ADDRESS}:{DATABASE_PORT}/{DATABASE_NAME}"
    template["create_table"] = "CREATE TABLE IF NOT EXISTS {} "
    template["drop_table"]   = "DROP TABLE IF EXISTS {} "
    
    if key in template:
        return template[key]
    else:
        raise Exception("{} hasn't been defined yet. Contact your administrator".format(key))


def fill_template(key, template, schemas_dict):
    """
    - Fill the template for actions in tables
    """
    comment = None
    
    fields = "( {} )"
    fields_value= ""
    if key in schemas_dict:
        if template == "drop_table":
            pass
        else:
            for k, v in schemas_dict[key].items():
                if k == "table_description":
                    comment = v
                else:
                    fields_value += "\n {} {},".format( k , v.replace("_", " ") )

    if len(fields_value) < 2:
        fields =""
                
    if comment:
        COMMENT= "; COMMENT ON TABLE {} IS '{}'".format(key, comment)
    else:
        COMMENT= ""
    

        
    ACTION = choose_template(template).format(key) +\
             fields.format(fields_value[:-1]) +\
             "\n" + COMMENT
    
    return ACTION