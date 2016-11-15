-- Figure out how many countries only won a single metal
athletes = LOAD '/Users/jmbeck/Documents/Learning/learning_pig/data/OlympicAthletes.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (athlete:chararray, country:chararray, year:int, sport:chararray, gold:int, silver:int, bronze:int, total:int);

athletes_by_country = GROUP athletes BY country; 
metals_bycountry = FOREACH athletes_by_country GENERATE group as country, SUM(athletes.total) as metals_won;
solo_winners = FILTER metals_bycountry BY metals_won == 1;
countries_group = GROUP solo_winners ALL;
country_count = FOREACH countries_group GENERATE COUNT(solo_winners);
DUMP country_count;