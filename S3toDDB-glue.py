import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="ms-from-s3-crawlerevents",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node Change Schema
ChangeSchema_node2 = DataCatalogtable_node1.apply_mapping(
#    frame=DataCatalogtable_node1,
    mappings=[
        ("col0", "long", "GlobalEventID", "long"),
        ("col1", "long", "Day", "long"),
        ("col2", "long", "MonthYear", "long"),
        ("col3", "long", "Year", "long"),
        ("col4", "double", "FractionDate", "double"),
        ("col5", "string", "Actor1Code", "string"),
        ("col6", "string", "Actor1Name", "string"),
        ("col7", "string", "Actor1CountryCode", "string"),
        ("col8", "string", "Actor1KnownGroupCode", "string"),
        ("col9", "string", "Actor1EthnicCode", "string"),
        ("col10", "string", "Actor1Religion1Code", "string"),
        ("col11", "string", "Actor1Religion2Code", "string"),
        ("col12", "string", "Actor1Type1Code", "string"),
        ("col13", "string", "Actor1Type2Code", "string"),
        ("col14", "string", "Actor1Type3Code", "string"),
        ("col15", "string", "Actor2Code", "string"),
        ("col16", "string", "Actor2Name", "string"),
        ("col17", "string", "Actor2CountryCode", "string"),
        ("col18", "string", "Actor2KnownGroupCode", "string"),
        ("col19", "string", "Actor2EthnicCode", "string"),
        ("col20", "string", "Actor2Religion1Code", "string"),
        ("col21", "string", "Actor2Religion2Code", "string"),
        ("col22", "string", "Actor2Type1Code", "string"),
        ("col23", "string", "Actor2Type2Code", "string"),
        ("col24", "string", "Actor2Type3Code", "string"),
        ("col25", "long", "IsRootEvent", "long"),
        ("col26", "long", "EventCode", "long"),
        ("col27", "long", "EventBaseCode", "long"),
        ("col28", "long", "EventRootCode", "long"),
        ("col29", "long", "QuadClass", "long"),
        ("col30", "double", "GoldsteinScale", "double"),
        ("col31", "long", "NumMentions", "long"),
        ("col32", "long", "NumSources", "long"),
        ("col33", "long", "NumArticles", "long"),
        ("col34", "double", "AvgTone", "double"),
        ("col35", "long", "Actor1Geo_Type", "long"),
        ("col36", "string", "Actor1Geo_Fullname", "string"),
        ("col37", "string", "Actor1Geo_CountryCode", "string"),
        ("col38", "string", "Actor1Geo_ADM1Code", "string"),
        ("col39", "double", "Actor1Geo_Lat", "double"),
        ("col40", "double", "Actor1Geo_Long", "double"),
        ("col41", "string", "Actor1Geo_FeatureID", "string"),
        ("col42", "long", "Actor2Geo_Type", "long"),
        ("col43", "string", "Actor2Geo_Fullname", "string"),
        ("col44", "string", "Actor2Geo_CountryCode", "string"),
        ("col45", "string", "Actor2Geo_ADM1Code", "string"),
        ("col46", "double", "Actor2Geo_Lat", "double"),
        ("col47", "double", "Actor2Geo_Long", "double"),
        ("col48", "string", "Actor2Geo_FeatureID", "string"),
        ("col49", "long", "ActionGeo_Type", "long"),
        ("col50", "string", "ActionGeo_Fullname", "string"),
        ("col51", "string", "ActionGeo_CountryCode", "string"),
        ("col52", "string", "ActionGeo_ADM1Code", "string"),
        ("col53", "double", "ActionGeo_Lat", "double"),
        ("col54", "double", "ActionGeo_Long", "double"),
        ("col55", "string", "ActionGeo_FeatureID", "string"),
        ("col56", "long", "DATEADDED", "long"),
        ("col57", "string", "SOURCEURL", "string"),
    ],
    transformation_ctx="ChangeSchema_node2",
)

glueContext.write_dynamic_frame_from_options ( frame = ChangeSchema_node2, connection_type = "dynamodb", connection_options = { "dynamodb.region": "us-east-1", "dynamodb.output.tableName": "milestone-table-glue-new", "dynamodb.throughput.write.percent": "1.5" } )

job.commit()
