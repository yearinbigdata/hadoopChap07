/*
 * ontime table
 *
 */
--drop table ontime;
--truncate table ontime;
--delete from ontime; 
CREATE TABLE ontime
(
   Year                int,
   Month               int,
   DayofMonth          int,
   DayOfWeek           int,
   DepTime             int,
   CRSDepTime          int,
   ArrTime             int,
   CRSArrTime          int,
   UniqueCarrier       varchar(5),
   FlightNum           int,
   TailNum             varchar(8),
   ActualElapsedTime   int,
   CRSElapsedTime      int,
   AirTime             int,
   ArrDelay            int,
   DepDelay            int,
   Origin              varchar(3),
   Dest                varchar(3),
   Distance            int,
   TaxiIn              int,
   TaxiOut             int,
   Cancelled           int,
   CancellationCode    varchar(1),
   Diverted            varchar(1),
   CarrierDelay        int,
   WeatherDelay        int,
   NASDelay            int,
   SecurityDelay       int,
   LateAircraftDelay   int
);

/*
 * airport table
 *
 */
drop table airport;
truncate airport;
delete from airport; 
create table airport (
	iata		varchar(10) PRIMARY KEY,
	airport	varchar(100),
	city		varchar(100),
	state		varchar(50),
	country	varchar(50),
	lat			numeric(11,8),
	lng			numeric(11,8)
); 

select * from airport;

select o.`Year`, o.`Origin`, a.iata, a.airport
  from ontime o inner join airport a
    on o.`Origin` = a.iata
 where o.`Year` = 1987;	

/*
 * carrier table
 *
 */
drop table carrier;
truncate carrier;
delete from carrier; 
create table carrier ( 
	Code			varchar(5) PRIMARY KEY ,
	Description	varchar(100)
);

select count(*) from carrier;

select o.`Year`, o.`UniqueCarrier`, c.`Code`, c.`Description`
  from ontime o inner join carrier c
    on o.`UniqueCarrier` = c.`Code`
 where o.`Year` = 1987;	
/*
 * plane table
 *
 */
drop table plane;
truncate plane;
delete from plane; 
create table plane (
	tailnum		varchar(8) PRIMARY KEY,
	type			varchar(100),
	manufacturer  varchar(100),
	issue_date		varchar(100),
	model			varchar(100),
	status			varchar(100),
	aircraft_type	varchar(100),
	engine_type	varchar(100),
	year			varchar(4)
); 

select * from plane;

--
-- select
--
select * from airport;
select * from carrier;
select * from plane;
select * from ontime;

select o.`Year`, o.`Month`, o.`Origin`, a1.iata, a1.airport, o.`Dest`, a2.airport
  from ontime o inner join airport a1 inner join airport a2
    on (o.`Origin` = a1.iata and o.`Dest` = a2.iata)
 limit 0, 10;
 
-- select o.`Year`, o.`Month`, o.`UniqueCarrier`, c.`Code`, c.`Description`
select c.`Description`, o.*
   from ontime o inner join carrier c
     on (o.`UniqueCarrier` = c.`Code`)
  limit 0, 10;






