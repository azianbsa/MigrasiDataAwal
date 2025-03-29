DROP TEMPORARY TABLE IF EXISTS __tmp_periode;
CREATE TEMPORARY TABLE __tmp_periode AS
SELECT
@id:=@id+1 AS idperiode,
periode
FROM
[bsbs].periode
,(SELECT @id:=0) AS id
ORDER BY periode;

SELECT
@idpdam,
pel.id AS idpelangganair,
per.idperiode AS idperiode,
IFNULL(rek.blok1, 0) AS blok1,
IFNULL(rek.blok2, 0) AS blok2,
IFNULL(rek.blok3, 0) AS blok3,
IFNULL(rek.blok4, 0) AS blok4,
IFNULL(rek.blok5, 0) AS blok5,
IFNULL(rek.prog1, 0) AS prog1,
IFNULL(rek.prog2, 0) AS prog2,
IFNULL(rek.prog3, 0) AS prog3,
IFNULL(rek.prog4, 0) AS prog4,
IFNULL(rek.prog5, 0) AS prog5
FROM
[table] rek
JOIN pelanggan pel ON pel.nosamb=rek.nosamb
JOIN __tmp_periode per ON per.periode=rek.periode
WHERE rek.periode=@periode
AND rek.flaglunas=1
AND rek.flagbatal=0
AND rek.flagangsur=0
AND DATE(rek.tglbayar)<=@cutoff