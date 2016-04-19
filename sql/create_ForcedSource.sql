create table ForcedSource (
       objectId BIGINT,
       ccdVisitId BIGINT,
       psFlux FLOAT,
       psFlux_Sigma FLOAT,
       flags TINYINT,
       primary key (objectId, ccdVisitID)
       )
