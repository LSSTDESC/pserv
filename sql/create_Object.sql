create table Object (
       objectId BIGINT,
       parentObjectId BIGINT,
       prv_inputId INT,
       psRa DOUBLE,
       psRaSigma FLOAT,
       psDecl DOUBLE,
       psDeclSigma FLOAT,
       psMuRa FLOAT,
       psMuRaSigma FLOAT,
       psMuDecl FLOAT,
       psMuDeclSigma FLOAT,
       psParallax FLOAT,
       psParallaxSigma FLOAT,
       uPsFlux FLOAT,
       uPsFluxSigma FLOAT,
       gPsFlux FLOAT,
       gPsFluxSigma FLOAT,
       rPsFlux FLOAT,
       rPsFluxSigma FLOAT,
       iPsFlux FLOAT,
       iPsFluxSigma FLOAT,
       zPsFlux FLOAT,
       zPsFluxSigma FLOAT,
       yPsFlux FLOAT,
       yPsFluxSigma FLOAT,
       psLnL FLOAT,
       psChi2 FLOAT,
       psN INT,
       uBbdRa DOUBLE,
       uBdRaSigma FLOAT,
       uBdDecl DOUBLE,
       uBdDeclSigma FLOAT,
       uBdE1 FLOAT,
       uBdE1Sigma FLOAT,
       uBdE2 FLOAT,
       uBdE2Sigma FLOAT,
       uBdFluxB FLOAT,
       uBdFluxBSigma FLOAT,
       uBdFluxD FLOAT,
       uBdFluxDSigma FLOAT,
       uBdReB FLOAT,
       uBdReBSigma FLOAT,
       uBdReD FLOAT,
       uBdReDSigma FLOAT,
       uBdLnL FLOAT,
       uBdChi2 FLOAT,
       uBdN INT,
       gBbdRa DOUBLE,
       gBdRaSigma FLOAT,
       gBdDecl DOUBLE,
       gBdDeclSigma FLOAT,
       gBdE1 FLOAT,
       gBdE1Sigma FLOAT,
       gBdE2 FLOAT,
       gBdE2Sigma FLOAT,
       gBdFluxB FLOAT,
       gBdFluxBSigma FLOAT,
       gBdFluxD FLOAT,
       gBdFluxDSigma FLOAT,
       gBdReB FLOAT,
       gBdReBSigma FLOAT,
       gBdReD FLOAT,
       gBdReDSigma FLOAT,
       gBdLnL FLOAT,
       gBdChi2 FLOAT,
       gBdN INT,
       rBbdRa DOUBLE,
       rBdRaSigma FLOAT,
       rBdDecl DOUBLE,
       rBdDeclSigma FLOAT,
       rBdE1 FLOAT,
       rBdE1Sigma FLOAT,
       rBdE2 FLOAT,
       rBdE2Sigma FLOAT,
       rBdFluxB FLOAT,
       rBdFluxBSigma FLOAT,
       rBdFluxD FLOAT,
       rBdFluxDSigma FLOAT,
       rBdReB FLOAT,
       rBdReBSigma FLOAT,
       rBdReD FLOAT,
       rBdReDSigma FLOAT,
       rBdLnL FLOAT,
       rBdChi2 FLOAT,
       rBdN INT,
       iBbdRa DOUBLE,
       iBdRaSigma FLOAT,
       iBdDecl DOUBLE,
       iBdDeclSigma FLOAT,
       iBdE1 FLOAT,
       iBdE1Sigma FLOAT,
       iBdE2 FLOAT,
       iBdE2Sigma FLOAT,
       iBdFluxB FLOAT,
       iBdFluxBSigma FLOAT,
       iBdFluxD FLOAT,
       iBdFluxDSigma FLOAT,
       iBdReB FLOAT,
       iBdReBSigma FLOAT,
       iBdReD FLOAT,
       iBdReDSigma FLOAT,
       iBdLnL FLOAT,
       iBdChi2 FLOAT,
       iBdN INT,
       zBbdRa DOUBLE,
       zBdRaSigma FLOAT,
       zBdDecl DOUBLE,
       zBdDeclSigma FLOAT,
       zBdE1 FLOAT,
       zBdE1Sigma FLOAT,
       zBdE2 FLOAT,
       zBdE2Sigma FLOAT,
       zBdFluxB FLOAT,
       zBdFluxBSigma FLOAT,
       zBdFluxD FLOAT,
       zBdFluxDSigma FLOAT,
       zBdReB FLOAT,
       zBdReBSigma FLOAT,
       zBdReD FLOAT,
       zBdReDSigma FLOAT,
       zBdLnL FLOAT,
       zBdChi2 FLOAT,
       zBdN INT,
       yBbdRa DOUBLE,
       yBdRaSigma FLOAT,
       yBdDecl DOUBLE,
       yBdDeclSigma FLOAT,
       yBdE1 FLOAT,
       yBdE1Sigma FLOAT,
       yBdE2 FLOAT,
       yBdE2Sigma FLOAT,
       yBdFluxB FLOAT,
       yBdFluxBSigma FLOAT,
       yBdFluxD FLOAT,
       yBdFluxDSigma FLOAT,
       yBdReB FLOAT,
       yBdReBSigma FLOAT,
       yBdReD FLOAT,
       yBdReDSigma FLOAT,
       yBdLnL FLOAT,
       yBdChi2 FLOAT,
       yBdN INT,
       ugStd FLOAT,
       ugStdSigma FLOAT,
       grStd FLOAT,
       grStdSigma FLOAT,
       riStd FLOAT,
       riStdSigma FLOAT,
       izStd FLOAT,
       izStdSigma FLOAT,
       zyStd FLOAT,
       zyStdSigma FLOAT,
       uRa DOUBLE,
       uRaSigma DOUBLE,
       uDecl DOUBLE,
       uDeclSigma DOUBLE,
       gRa DOUBLE,
       gRaSigma DOUBLE,
       gDecl DOUBLE,
       gDeclSigma DOUBLE,
       rRa DOUBLE,
       rRaSigma DOUBLE,
       rDecl DOUBLE,
       rDeclSigma DOUBLE,
       iRa DOUBLE,
       iRaSigma DOUBLE,
       iDecl DOUBLE,
       iDeclSigma DOUBLE,
       zRa DOUBLE,
       zRaSigma DOUBLE,
       zDecl DOUBLE,
       zDeclSigma DOUBLE,
       yRa DOUBLE,
       yRaSigma DOUBLE,
       yDecl DOUBLE,
       yDeclSigma DOUBLE,
       uE1 FLOAT,
       uE1Sigma FLOAT,
       uE2 FLOAT,
       uE2Sigma FLOAT,
       uE1_E2_Cov FLOAT,
       gE1 FLOAT,
       gE1Sigma FLOAT,
       gE2 FLOAT,
       gE2Sigma FLOAT,
       gE1_E2_Cov FLOAT,
       rE1 FLOAT,
       rE1Sigma FLOAT,
       rE2 FLOAT,
       rE2Sigma FLOAT,
       rE1_E2_Cov FLOAT,
       iE1 FLOAT,
       iE1Sigma FLOAT,
       iE2 FLOAT,
       iE2Sigma FLOAT,
       iE1_E2_Cov FLOAT,
       zE1 FLOAT,
       zE1Sigma FLOAT,
       zE2 FLOAT,
       zE2Sigma FLOAT,
       zE1_E2_Cov FLOAT,
       yE1 FLOAT,
       yE1Sigma FLOAT,
       yE2 FLOAT,
       yE2Sigma FLOAT,
       yE1_E2_Cov FLOAT,
       uMSum FLOAT,
       uMSumSigma FLOAT,
       gMSum FLOAT,
       gMSumSigma FLOAT,
       rMSum FLOAT,
       rMSumSigma FLOAT,
       iMSum FLOAT,
       iMSumSigma FLOAT,
       zMSum FLOAT,
       zMSumSigma FLOAT,
       yMSum FLOAT,
       yMSumSigma FLOAT,
       uM4 FLOAT,
       gM4 FLOAT,
       rM4 FLOAT,
       iM4 FLOAT,
       zM4 FLOAT,
       yM4 FLOAT,
       uPetroRad FLOAT,
       uPetroRadSigma FLOAT,
       gPetroRad FLOAT,
       gPetroRadSigma FLOAT,
       rPetroRad FLOAT,
       rPetroRadSigma FLOAT,
       iPetroRad FLOAT,
       iPetroRadSigma FLOAT,
       zPetroRad FLOAT,
       zPetroRadSigma FLOAT,
       yPetroRad FLOAT,
       yPetroRadSigma FLOAT,
       petroFilter CHAR(1),
       uPetroFlux FLOAT,
       uPetroFluxSigma FLOAT,
       gPetroFlux FLOAT,
       gPetroFluxSigma FLOAT,
       rPetroFlux FLOAT,
       rPetroFluxSigma FLOAT,
       iPetroFlux FLOAT,
       iPetroFluxSigma FLOAT,
       zPetroFlux FLOAT,
       zPetroFluxSigma FLOAT,
       yPetroFlux FLOAT,
       yPetroFluxSigma FLOAT,
       uPetroRad50 FLOAT,
       uPetroRad50Sigma FLOAT,
       gPetroRad50 FLOAT,
       gPetroRad50Sigma FLOAT,
       rPetroRad50 FLOAT,
       rPetroRad50Sigma FLOAT,
       iPetroRad50 FLOAT,
       iPetroRad50Sigma FLOAT,
       zPetroRad50 FLOAT,
       zPetroRad50Sigma FLOAT,
       yPetroRad50 FLOAT,
       yPetroRad50Sigma FLOAT,
       uPetroRad90 FLOAT,
       uPetroRad90Sigma FLOAT,
       gPetroRad90 FLOAT,
       gPetroRad90Sigma FLOAT,
       rPetroRad90 FLOAT,
       rPetroRad90Sigma FLOAT,
       iPetroRad90 FLOAT,
       iPetroRad90Sigma FLOAT,
       zPetroRad90 FLOAT,
       zPetroRad90Sigma FLOAT,
       yPetroRad90 FLOAT,
       yPetroRad90Sigma FLOAT,
       uKronRad FLOAT,
       uKronRadSigma FLOAT,
       gKronRad FLOAT,
       gKronRadSigma FLOAT,
       rKronRad FLOAT,
       rKronRadSigma FLOAT,
       iKronRad FLOAT,
       iKronRadSigma FLOAT,
       zKronRad FLOAT,
       zKronRadSigma FLOAT,
       yKronRad FLOAT,
       yKronRadSigma FLOAT,
       kronFilter CHAR(1),
       uKronFlux FLOAT,
       uKronFluxSigma FLOAT,
       gKronFlux FLOAT,
       gKronFluxSigma FLOAT,
       rKronFlux FLOAT,
       rKronFluxSigma FLOAT,
       iKronFlux FLOAT,
       iKronFluxSigma FLOAT,
       zKronFlux FLOAT,
       zKronFluxSigma FLOAT,
       yKronFlux FLOAT,
       yKronFluxSigma FLOAT,
       uKronRad50 FLOAT,
       uKronRad50Sigma FLOAT,
       gKronRad50 FLOAT,
       gKronRad50Sigma FLOAT,
       rKronRad50 FLOAT,
       rKronRad50Sigma FLOAT,
       iKronRad50 FLOAT,
       iKronRad50Sigma FLOAT,
       zKronRad50 FLOAT,
       zKronRad50Sigma FLOAT,
       yKronRad50 FLOAT,
       yKronRad50Sigma FLOAT,
       uKronRad90 FLOAT,
       uKronRad90Sigma FLOAT,
       gKronRad90 FLOAT,
       gKronRad90Sigma FLOAT,
       rKronRad90 FLOAT,
       rKronRad90Sigma FLOAT,
       iKronRad90 FLOAT,
       iKronRad90Sigma FLOAT,
       zKronRad90 FLOAT,
       zKronRad90Sigma FLOAT,
       yKronRad90 FLOAT,
       yKronRad90Sigma FLOAT,
       uApN TINYINT,
       gApN TINYINT,
       rApN TINYINT,
       iApN TINYINT,
       zApN TINYINT,
       yApN TINYINT,
       extendedness FLOAT,
       FLAGS1 BIGINT,
       FLAGS2 BIGINT,
       primary key (objectId)
       )
