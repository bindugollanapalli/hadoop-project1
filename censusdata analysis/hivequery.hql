
use census1;


drop table census_demo;
drop table agegroup;
drop table censusdata;


create table census_demo(jsondata string) row format delimited stored as textfile;

create table agegroup(age int,agegroup String) row format delimited fields terminated by '\t' stored as textfile;

create table censusdata(age int,education string,maritalstatus string,gender string,taxfilerstatus string,income double,parents string,countryofbirth string,citizenship string,weekworked int) row format delimited fields terminated by ',' stored as textfile;

LOAD DATA INPATH '/censusproject/censusdata/agegroup.dat' into table agegroup;

LOAD DATA INPATH '/censusproject/censusdata/sample.dat' OVERWRITE INTO TABLE census_demo;

insert overwrite table censusdata select get_json_object(jsondata,"$.Age"),get_json_object(jsondata,"$.Education"),get_json_object(jsondata,"$.MaritalStatus"),get_json_object(jsondata,"$.Gender"),get_json_object(jsondata,"$.TaxFilerStatus"),get_json_object(jsondata,"$.Income"),get_json_object(jsondata,"$.Parents"),get_json_object(jsondata,"$.CountryOfBirth"),get_json_object(jsondata,"$.Citizenship"),get_json_object(jsondata,"$.WeeksWorked") from census_demo;

INSERT OVERWRITE DIRECTORY '/censusproject/Data/censusdata' row format delimited fields terminated by ',' select * from censusdata;

