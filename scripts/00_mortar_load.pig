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


-- Find the top 5 countries by metal count
athletes_by_country = GROUP athletes BY country; 
metals_bycountry = FOREACH athletes_by_country GENERATE group as country, SUM(athletes.total) as metals_won;
metals_by_country_sorted = ORDER metals_bycountry BY metals_won DESC;
top5_winningest = LIMIT metals_by_country_sorted 5; 

DUMP top5_winningest;

-- Get rid of swimming in the dataset and recalculate
no_swimmers = FILTER athletes BY sport != 'Swimming';
athletes_by_country = GROUP no_swimmers BY country; 
metals_bycountry = FOREACH athletes_by_country GENERATE group as country, SUM(athletes.total) as metals_won;
metals_by_country_sorted = ORDER metals_bycountry BY metals_won DESC;
top5_winningest = LIMIT metals_by_country_sorted 5; 


-- Figure out how many countries only won a single metal
athletes_by_country = GROUP athletes BY country; 
metals_bycountry = FOREACH athletes_by_country GENERATE group as country, SUM(athletes.total) as metals_won;
solo_winners = FILTER metals_bycountry BY metals_won == 1;
countries_group = GROUP solo_winners ALL;
country_count = FOREACH countries_group GENERATE COUNT(solo_winners);
DUMP country_count;

