athletes = LOAD '/Users/jmbeck/Documents/Learning/learning_pig/data/OlympicAthletes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (athlete:chararray, country:chararray, year:int, sport:chararray, gold:int, silver:int, bronze:int, total:int);

DUMP athletes; 

athletes_lim = LIMIT athletes 10;

-- Group by country
athletes_by_country = GROUP athletes BY country; 

DESCRIBE athletes_by_country;

-- Calculate total metals by country
metals_bycountry = FOREACH athletes_by_country GENERATE group as country, SUM(athletes.total) as metals_won;

-- Find out the min and max year in the data set

years = GROUP athletes ALL;

min_max_years = FOREACH years GENERATE MIN(athletes.year) as min_year, MAX(athletes.year) as max_year;


-- Grab a list of distinct countries in the data set
country_list = DISTINCT (FOREACH athletes GENERATE country);





