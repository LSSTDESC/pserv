create table if not exists ForcedSourceExtra (
       objectId BIGINT,
       ccdVisitId BIGINT,
       ap_3_0_Flux FLOAT,
       ap_3_0_Flux_Sigma FLOAT,
       ap_9_0_Flux FLOAT,
       ap_9_0_Flux_Sigma FLOAT,
       ap_17_0_Flux FLOAT,
       ap_17_0_Flux_Sigma FLOAT,
       ap_25_0_Flux FLOAT,
       ap_25_0_Flux_Sigma FLOAT,
       ap_50_0_Flux FLOAT,
       ap_50_0_Flux_Sigma FLOAT,
       flags TINYINT,
       projectId INTEGER,
       primary key (objectId, ccdVisitID, projectId)
       )
