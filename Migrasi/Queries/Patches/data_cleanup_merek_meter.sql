UPDATE pelanggan SET merkmeter='-' WHERE merkmeter='';

REPLACE INTO merkmeter (merk)
SELECT
merkmeter
FROM pelanggan
WHERE merkmeter NOT IN (SELECT merk FROM merkmeter)
GROUP BY merkmeter;