--Схема измерений
create schema dim;
--Схема фактов
create schema fact;
--Схема логирования ETL-процессов
create schema etl_log;

--Таблица измерений airports
create table dim.airports
(
	id serial primary key,
	airport_code bpchar(3) not null,
	airport_name text null,
	city text null,
	longitude float8 null,
	latitude float8 null,
	timezone text null
);

--Таблица измерений aircrafts
create table dim.aircrafts (
	id serial primary key,
	aircraft_code bpchar(3) not null,
	model text null,
	"range" int4 null
);

--Таблица измерений passengers
create table dim.passengers (
	id serial primary key,
	document_id varchar(20) not null,
	passenger_name text null,
	phone varchar(20) NULL,
	email varchar(100) null
);

--Таблица измерений tariff
create table dim.tariff (
	id serial primary key,
	fare_conditions varchar(10)
);

--Таблица измерений calendar
create table dim.calendar
AS
WITH dates AS (
    SELECT dd::date AS dt
    FROM generate_series
            ('2010-01-01'::timestamp
            , '2030-01-01'::timestamp
            , '1 day'::interval) dd
)
SELECT
    to_char(dt, 'YYYYMMDD')::int AS id,
    dt AS date,
    to_char(dt, 'YYYY-MM-DD') AS ansi_date,
    date_part('isodow', dt)::int AS day,
    date_part('week', dt)::int AS week_number,
    date_part('month', dt)::int AS month,
    date_part('isoyear', dt)::int AS year,
    (date_part('isodow', dt)::smallint BETWEEN 1 AND 5)::int AS week_day,
    (to_char(dt, 'YYYYMMDD')::int IN (
        20130101,
        20130102,
        20130103,
        20130104,
        20130105,
        20130106,
        20130107,
        20130108,
        20130223,
        20130308,
        20130310,
        20130501,
        20130502,
        20130503,
        20130509,
        20130510,
        20130612,
        20131104,
        20140101,
        20140102,
        20140103,
        20140104,
        20140105,
        20140106,
        20140107,
        20140108,
        20140223,
        20140308,
        20140310,
        20140501,
        20140502,
        20140509,
        20140612,
        20140613,
        20141103,
        20141104,
        20150101,
        20150102,
        20150103,
        20150104,
        20150105,
        20150106,
        20150107,
        20150108,
        20150109,
        20150223,
        20150308,
        20150309,
        20150501,
        20150504,
        20150509,
        20150511,
        20150612,
        20151104,
        20160101,
        20160102,
        20160103,
        20160104,
        20160105,
        20160106,
        20160107,
        20160108,
        20160222,
        20160223,
        20160307,
        20160308,
        20160501,
        20160502,
        20160503,
        20160509,
        20160612,
        20160613,
        20161104,
        20170101,
        20170102,
        20170103,
        20170104,
        20170105,
        20170106,
        20170107,
        20170108,
        20170223,
        20170224,
        20170308,
        20170501,
        20170508,
        20170509,
        20170612,
        20171104,
        20171106,
        20180101,
        20180102,
        20180103,
        20180104,
        20180105,
        20180106,
        20180107,
        20180108,
        20180223,
        20180308,
        20180309,
        20180430,
        20180501,
        20180502,
        20180509,
        20180611,
        20180612,
        20181104,
        20181105,
        20181231,
        20190101,
        20190102,
        20190103,
        20190104,
        20190105,
        20190106,
        20190107,
        20190108,
        20190223,
        20190308,
        20190501,
        20190502,
        20190503,
        20190509,
        20190510,
        20190612,
        20191104,
        20200101, 20200102, 20200103, 20200106, 20200107, 20200108,
       20200224, 20200309, 20200501, 20200504, 20200505, 20200511,
       20200612, 20201104))::int AS holiday
FROM dates
ORDER BY dt;

ALTER TABLE dim.calendar ADD PRIMARY KEY (id);


--Таблица фактов перелетов
create table fact.flights
( 
	id serial primary key,
	flight_id int4 not null,
	flight_no bpchar(6) not null,
	passenger_id int4 not null references dim.passengers(id),
	actual_departure timestamp not null,
	actual_arrival timestamp not null,
	departure_delay bigint not null,
	arrival_delay bigint not null,
	aircraft_id int4 not null references dim.aircrafts(id),
	departure_airport_id int4 not null references dim.airports(id),
	arrival_airport_id int4 not null references dim.airports(id),
	tariff_id int4 not null references dim.tariff(id),
	amount numeric(10,2) not null
);

--rejected-таблица для ETL самолетов
create table etl_log.aircrafts_bad_events
( 
	id serial primary key,
	aircraft_code bpchar(3) not null,
	model text not null,
	"range" int4 not null,
	message text
);

--rejected-таблица для ETL аэропортов
create table etl_log.airports_bad_events
(
	id serial primary key,
	airport_code bpchar(3) not null,
	airport_name text not null,
	city text not null,
	longitude float8 not null,
	latitude float8 not null,
	timezone text not null,
	message text
);

--rejected-таблица для ETL пассажиров
create table etl_log.passengers_bad_events (
	id serial primary key,
	document_id varchar(20) not null,
	passenger_name text not null,
	phone varchar(20) NULL,
	email varchar(100) null,
	message text
);

--rejected-таблица для ETL тарифов
create table etl_log.tariff_bad_events (
	id serial primary key,
	fare_conditions varchar(10),
	message text
);

--rejected-таблица для ETL фактов перелетов
create table etl_log.flights_bad_events
( 
	id serial primary key,
	flight_id int4 null,
	flight_no bpchar(6) null,
	actual_departure timestamp null,
	actual_arrival timestamp null,
	departure_delay bigint null,
	arrival_delay bigint null,
	aircraft_code bpchar(3) null,
	departure_airport bpchar(3) null,
	arrival_airport bpchar(3) null,
	fare_conditions varchar(10) null,
	amount numeric(10,2) null,
	document_id varchar(20) null,
	message text
);
