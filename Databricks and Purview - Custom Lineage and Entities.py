# Databricks notebook source
# Requirement: pyapacheatlas

# COMMAND ----------

import os, json, jmespath
from pyapacheatlas.core import AtlasEntity
from pyapacheatlas.core.util import GuidTracker
from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess, TypeCategory
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core.typedef import AtlasAttributeDef, EntityTypeDef, RelationshipTypeDef
from pyspark.sql.types import StructType,StructField, StringType, ArrayType
from pyspark.sql.functions import explode, expr

# COMMAND ----------

# Add your credentials here or set them as environment variables
tenant_id = ""                      # Azure Tenant ID
client_id = ""                      # Service Principal Client ID
client_secret = ""                  # Service Principal Client Secret
purview_account_name = ""           # Purview Account Name

# COMMAND ----------

oauth = ServicePrincipalAuthentication(
        tenant_id=os.environ.get("TENANT_ID", tenant_id),
        client_id=os.environ.get("CLIENT_ID", client_id),
        client_secret=os.environ.get("CLIENT_SECRET", client_secret)
    )
client = PurviewClient(
    account_name = os.environ.get("PURVIEW_NAME", purview_account_name),
    authentication=oauth
)
guid = GuidTracker()

# COMMAND ----------

# ---------------------------------------------------
# Purview API - Initial Setup Function
# ---------------------------------------------------

def setupDeltaEntities():
  
  # Create Delta Dataframe Type
  type_delta_df = EntityTypeDef(  
  name="delta_dataframe",
  attributeDefs=[
    AtlasAttributeDef(name="format")
  ],
  superTypes = ["DataSet"],
  options = {"schemaElementAttribute":"columns"}
   )
  
  # Create Delta Dataframe Column Type
  type_delta_columns = EntityTypeDef(
    name="delta_dataframe_column",
    attributeDefs=[
      AtlasAttributeDef(name="data_type")
    ],
    superTypes = ["DataSet"],
  )
  
  # Create Delta Job Process (for lineage relationships)
  type_delta_job = EntityTypeDef(
    name="delta_lake_notebook_process",
    attributeDefs=[
      AtlasAttributeDef(name="job_type",isOptional=False),
      AtlasAttributeDef(name="schedule",defaultValue=""),
      AtlasAttributeDef(name="notebook",defaultValue="")
    ],
    superTypes = ["Process"]
  )
  
  # Create relationship between Entities and Entity Columns
  delta_column_to_df_relationship = RelationshipTypeDef(
    name="delta_dataframe_to_columns",
    relationshipCategory="COMPOSITION",
    endDef1={
      "type": "delta_dataframe",
      "name": "columns",
      "isContainer": True,
      "cardinality": "SET",
      "isLegacyAttribute": False
      },
    endDef2={
      "type": "delta_dataframe_column",
      "name": "dataframe",
      "isContainer": False,
      "cardinality": "SINGLE",
      "isLegacyAttribute": False
      }
  )
  
  # Upload definitions
  typedef_results = client.upload_typedefs(
    entityDefs = [type_delta_df, type_delta_columns, type_delta_job ],
    relationshipDefs = [delta_column_to_df_relationship],
    force_update=True)

  return typedef_results

# COMMAND ----------

def getEntityGuid( entityQualifiedName, entityTypeName ):
  
  # Check Purview connection
  assert client.is_purview == True
  
  # Check entity guid (for further queries)
  selectedGuid = client.get_entity(qualifiedName=entityQualifiedName, typeName=entityTypeName)
  
  return selectedGuid

# COMMAND ----------

# ---------------------------------------------------
# Purview API - Register Delta Entity Function
# ---------------------------------------------------

def registerDeltaEntityAndColumns(df, entityName: str, entityQualifiedName: str):
  
  # Create Entity
  atlas_input_df = AtlasEntity(
    # Entity Name
    name=entityName,
    # Qualified Name (Full Path Name e.g "https://*****.blob.core.windows.net/adventureworks/Address/Year={N}/Month={N}/Day={N}/Time={N}-{N}/Address.json")
    qualified_name = entityQualifiedName,
    # Entity Type (delta_dataframe)
    typeName="delta_dataframe",
    # Create unique GUID if new
    guid=guid.get_guid(),
  )
  
  # Create list to loop through columns
  atlas_input_df_columns = []
  
  for column in df.schema:
    temp_column = AtlasEntity(
      # Set column name
      name = column.name,
      # Set Entity Type
      typeName = "delta_dataframe_column",
      # Set Qualified Name
      qualified_name = f"{entityQualifiedName}#{column.name}",
      # Create unique GUID if new
      guid=guid.get_guid(),
      # Set attributes
      attributes = {"data_type":str(column.dataType)},
      relationshipAttributes = {"dataframe":atlas_input_df.to_json(minimum=True)}
    )
    atlas_input_df_columns.append(temp_column)
  
  # Update Catalog
  return client.upload_entities([atlas_input_df]+ atlas_input_df_columns)

# COMMAND ----------

# ------------------------------------------------------------------------
# Purview API - Register Lineage Between Entities Function
# ------------------------------------------------------------------------

def registerDeltaEntityLineage(processName, entityQualifiedNameSource, entityTypeNameSource, entityQualifiedNameDest, entityTypeNameDest):
  
  try:
    entity1 = getEntityGuid(entityQualifiedName=entityQualifiedNameSource, entityTypeName=entityTypeNameSource)
    assert len(entity1.items()) > 0
  except:
    raise Exception(f"Entity cannot be found in catalog for {entityQualifiedNameSource}")
    
  try:
    entity2 = getEntityGuid(entityQualifiedName=entityQualifiedNameDest, entityTypeName=entityTypeNameDest)
    assert len(entity2.items()) > 0
  except:
    raise Exception(f"Entity cannot be found in catalog {entityQualifiedNameDest}")
    
  process_qn = f"{processName}: This can be a process name/path"

  newLineage = AtlasProcess(
      name=processName,
      typeName="delta_lake_notebook_process",
      qualified_name=process_qn,
      attributes = {
        "job_type":"notebook", 
        "notebook": "notebook name"
      },
      inputs=[entity1['entities'][0]],
      outputs=[entity2['entities'][0]],
      guid=guid.get_guid()
  )

  results = client.upload_entities(
      batch=[newLineage]
  )

  return (json.dumps(results, indent=2))

