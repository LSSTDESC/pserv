create table if not exists ForcedSource (
       objectId BIGINT,
       ccdVisitId BIGINT,
       psFlux FLOAT,
       psFlux_Sigma FLOAT,
       flags TINYINT,
       projectId INTEGER,
       primary key (objectId, ccdVisitID, projectId)
       )
