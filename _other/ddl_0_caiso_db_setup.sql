--
--  psql -U postgres -f ddl_0_caiso_db_setup.sql
--
--  psql -U postgres -f ddl_0_caiso_db_setup.sql >> /opt/_lab/caiso/_log/ddl_0.log
--
--  TODO setup new datagrip connect to caiso
--

drop database if exists caiso with (force);

create database caiso;

\c caiso;

drop schema if exists stg cascade;
drop schema if exists core cascade;
drop schema if exists pres cascade;

create schema stg;
create schema core;
create schema pres;


create table stg.pay0 (
    id          serial primary key
   ,payload     varchar
   ,dw_ins_ts   timestamp with time zone default current_timestamp
   ,dw_upd_ts   timestamp with time zone default current_timestamp
   ,dw_file     varchar                  default ''
   ,dw_note     varchar                  default ''
);

create table stg.pay10 (
    id          serial primary key
   ,payload     varchar
   ,dw_ins_ts   timestamp with time zone default current_timestamp
   ,dw_upd_ts   timestamp with time zone default current_timestamp
   ,dw_file     varchar                  default ''
   ,dw_note     varchar                  default ''
);

create table stg.pay20 (
    id          serial primary key
   ,payload     varchar
   ,dw_ins_ts   timestamp with time zone default current_timestamp
   ,dw_upd_ts   timestamp with time zone default current_timestamp
   ,dw_file     varchar                  default ''
   ,dw_note     varchar                  default ''
);



create table core.ems_load_hourly_hist (
    id                 serial primary key
   ,ts0                timestamp without time zone default null
   ,dt0                date
   ,he                 numeric 
   ,pge                numeric
   ,sce                numeric
   ,sdge               numeric
   ,vea                numeric
   ,caiso_total        numeric
   ,pay_sha256         bytea
   ,dw_ins_ts          timestamp with time zone default current_timestamp
   ,dw_upd_ts          timestamp with time zone default current_timestamp
   ,dw_file            varchar                  default ''
   ,dw_note            varchar                  default ''
);

create table core.load_1_hr_hist (
    id                 serial primary key
   ,ts0                timestamp without time zone default null
   ,caiso_load         numeric
   ,pay_sha256         bytea
   ,dw_ins_ts          timestamp with time zone default current_timestamp
   ,dw_upd_ts          timestamp with time zone default current_timestamp
   ,dw_file            varchar                  default ''
   ,dw_note            varchar                  default ''
);

--create table core.fuel_mix_5_min_hist (
create table core.fuel_mix_1_hr_hist (
    id                 serial primary key
    ,ts0               timestamp without time zone default null
    ,solar             numeric
    ,wind              numeric
    ,geothermal        numeric
    ,biomass           numeric
    ,biogas            numeric
    ,small_hydro       numeric
    ,coal              numeric
    ,nuclear           numeric
    ,natural_gas       numeric
    ,large_hydro       numeric
    ,batteries         numeric
    ,imports           numeric
    ,other             numeric
    ,pay_sha256        bytea
    ,dw_ins_ts         timestamp with time zone default current_timestamp
    ,dw_upd_ts         timestamp with time zone default current_timestamp
    ,dw_file           varchar                  default ''
    ,dw_note           varchar                  default ''
);





-- NOTE remember to vacuum!
-- vacuum;


insert into stg.pay0 (payload,dw_file) values ('test','foo');

select * from stg.pay0 ;

delete from stg.pay0 ;



