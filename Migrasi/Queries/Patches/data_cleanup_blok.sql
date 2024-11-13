REPLACE INTO blok
SELECT
 id,
 koderayon,
 namarayon,
 koderayon
FROM
 rayon;
 
UPDATE pelanggan SET kodeblok=koderayon;