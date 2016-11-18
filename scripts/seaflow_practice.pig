-- Test reading in a new script
seaflow = LOAD '/Users/jmbeck/Documents/Learning/UW_materials/datasci_course_materials/assignment5/seaflow_21min.csv' USING
org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS
(file_id:int, time:int, cell_id:int, d1:int, d2:int, fsc_small:int, fsc_perp:int, fsc_big:int, pe:int, chl_small:int,
chl_big:int, pop:chararray);

-- Basic data load
--seaflow = LOAD '/Users/jmbeck/Documents/Learning/UW_materials/datasci_course_materials/assignment5/seaflow_21min.csv' USING PigStorage(',');

-- Give distinct pop names and counts

pops = GROUP seaflow BY pop;

pop_counts = FOREACH pops GENERATE group as pop, COUNT(seaflow) AS total_records;

pop_counts = ORDER pop_counts BY total_records DESC;

DUMP pop_counts;

