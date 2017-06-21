create table if not exists GalaxyTruth (
       id BIGINT,
       raICRS DOUBLE,
       decICRS DOUBLE,
       u_mag FLOAT,
       g_mag FLOAT,
       r_mag FLOAT,
       i_mag FLOAT,
       z_mag FLOAT,
       y_mag FLOAT,
       redshift FLOAT,
       majorAxis FLOAT,
       minorAxis FLOAT,
       positionAngle FLOAT,
       sindex FLOAT,
       primary key (id)
       )
