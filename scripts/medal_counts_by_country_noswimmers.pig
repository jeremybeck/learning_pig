-- Total medals with No Swimmers
athletes = LOAD '/Users/jmbeck/Documents/Learning/learning_pig/data/OlympicAthletes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (athlete:chararray, country:chararray, year:int, sport:chararray, gold:int, silver:int, bronze:int, total:int);

no_swimmers = FILTER athletes BY sport != 'Swimming';
athletes_by_country = GROUP no_swimmers BY country; 
medals_bycountry = FOREACH athletes_by_country GENERATE group as country, SUM(athletes.total) as medals_won;
medals_by_country_sorted = ORDER medals_bycountry BY medals_won DESC;
top5_winningest = LIMIT medals_by_country_sorted 5; 