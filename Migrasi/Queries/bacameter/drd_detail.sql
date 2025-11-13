/*
DROP TEMPORARY TABLE IF EXISTS __tmp_periode;
CREATE TEMPORARY TABLE __tmp_periode AS
SELECT
@id:=@id+1 AS idperiode,
periode
FROM
periode
,(SELECT @id:=0) AS id
ORDER BY periode;
*/

SELECT
@idpdam AS idpdam,
pm.id AS idpelangganair,
per.id AS idperiode,
IFNULL(rek.vol1, 0) AS blok1,
IFNULL(rek.vol2, 0) AS blok2,
IFNULL(rek.vol3, 0) AS blok3,
IFNULL(rek.vol4, 0) AS blok4,
0 AS blok5,
IFNULL(rek.tarif1, 0) AS prog1,
IFNULL(rek.tarif2, 0) AS prog2,
IFNULL(rek.tarif3, 0) AS prog3,
IFNULL(rek.tarif4, 0) AS prog4,
0 AS prog5
FROM [bacameter].`tbl_rekair` rek
JOIN [bacameter].`tbl_samb` pel ON pel.nosamb = rek.nosamb
JOIN `periode_map` per ON per.periode = CONCAT(rek.tahun, LPAD(rek.bulan,2,'0'))
LEFT JOIN pelanggan_map pm ON rek.nosamb=pm.nosamb
where rek.tahun=2025 and rek.bulan=9