COPY smoking FROM '/data/smoking.csv' DELIMITER AS ',' CSV HEADER;
SELECT * FROM smoking LIMIT 5;