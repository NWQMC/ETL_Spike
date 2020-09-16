create table if not exists ${WQX_SCHEMA_NAME}.dql_hierarchy
(hierarchy_value                numeric
,dqltyp_uid                     numeric
,dqltyp_name                    character varying(35)
,constraint dql_hierarchy_pk primary key (hierarchy_value)
,constraint dql_hierarchy_ak unique (dqltyp_uid)
)
with (fillfactor = 100)
