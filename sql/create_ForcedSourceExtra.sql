create table ForcedSourceExtra (
       objectId BIGINT,
       ccdVisitId BIGINT,
       ap_3_0_Flux FLOAT,
       ap_3_0_Flux_Sigma FLOAT,
       ap_9_0_Flux FLOAT,
       ap_9_0_Flux_Sigma FLOAT,
       ap_25_0_Flux FLOAT,
       ap_25_0_Flux_Sigma FLOAT,
       psf_apCorr_Flux FLOAT,
       psf_apCorr_Flux_Sigma FLOAT,
       gauss_apCorr_Flux FLOAT,
       gauss_apCorr_Flux_Sigma FLOAT,
       flags TINYINT,
       project CHAR(30),
       primary key (objectId, ccdVisitID, project)
       )
