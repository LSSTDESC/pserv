create table if not exists ForcedSource (
       objectId BIGINT,
       ccdVisitId BIGINT,
       psFlux FLOAT,
       psFlux_Sigma FLOAT,
       flags TINYINT,
       project CHAR(30),
       primary key (objectId, ccdVisitID, project)
       )
