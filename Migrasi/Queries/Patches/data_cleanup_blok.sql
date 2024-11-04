REPLACE INTO blok
SELECT
 id,
 koderayon,
 namarayon,
 koderayon
FROM
 rayon;
 
UPDATE pelanggan a
JOIN pelanggan b ON b.nosamb=a.nosamb
SET a.kodeblok=b.koderayon;