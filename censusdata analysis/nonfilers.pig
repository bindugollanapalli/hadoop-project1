censusdata = LOAD '/home/hduser/Documents/censusdata/census' USING PigStorage(',') AS (age:INT,education:CHARARRAY,maritalstatus:CHARARRAY,gender:CHARARRAY,taxfilestatus:CHARARRAY,income:DOUBLE,
parents:CHARARRAY,country:CHARARRAY,citizen:CHARARRAY,weeksworked:INT);
filterstep1 = FILTER censusdata BY weeksworked>0;
filterstep2 = FILTER filterstep1 BY taxfilestatus == ' Nonfiler';
groupallstep = GROUP filterstep2 ALL;
finalcount = FOREACH groupallstep GENERATE COUNT(filterstep2.taxfilestatus);
dump finalcount;

