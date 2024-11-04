UPDATE pelanggan SET kodesumberair='-' WHERE kodesumberair='';

REPLACE INTO sumberair (kodesumberair, sumberair)
SELECT
kodesumberair,
kodesumberair
FROM pelanggan
WHERE kodesumberair NOT IN (SELECT kodesumberair FROM sumberair)
GROUP BY kodesumberair;