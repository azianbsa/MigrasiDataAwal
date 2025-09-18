SELECT
@idpdam AS idpdam,
pel.idsamb AS idpelangganair,
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
WHERE rek.tahun='2025' AND rek.bulan='7';