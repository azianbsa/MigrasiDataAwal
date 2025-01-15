DROP TEMPORARY TABLE IF EXISTS temp_dataawal_periode;
CREATE TEMPORARY TABLE temp_dataawal_periode (
idperiode INT,
periode VARCHAR(10),
INDEX idx_temp_dataawal_periode_periode (periode)
);
INSERT INTO temp_dataawal_periode
SELECT
@idperiode:=@idperiode+1 AS idperiode,
periode
FROM [bsbs].periode
,(SELECT @idperiode:=0) AS idperiode
ORDER BY periode;

SELECT
@id := @id+1 AS id,
@idpdam,
NULL AS idangsuran,
per.idperiode AS idperiode,
pel.id AS idpelangganair,
rek.termin AS termin,
IF(rek.flagbatal=0,rek.flaglunas,0) AS statustransaksi,
rek.noangsuran AS nomortransaksi,
rek.waktubayar AS waktutransaksi,
YEAR(rek.waktubayar) AS tahuntransaksi,
NULL AS iduser,
NULL AS idloket,
NULL AS idkolektiftransaksi,
NULL AS idalasanbatal,
NULL AS keterangan,
ray.id AS idrayon,
kel.id AS idkelurahan,
gol.id AS idgolongan,
dia.id AS iddiameter,
DATE_FORMAT(DATE_ADD(STR_TO_DATE(CONCAT(rek.periode,'01'), '%Y%m%d'), INTERVAL 1 MONTH), '%Y-%m-01') AS tglmulaitagih,
IFNULL(rek.biayapemakaian, 0) AS biayapemakaian,
IFNULL(rek.administrasi, 0) AS administrasi,
IFNULL(rek.pemeliharaan, 0) AS pemeliharaan,
IFNULL(rek.retribusi, 0) AS retribusi,
0 AS pelayanan,
0 AS airlimbah,
0 AS dendapakai0,
0 AS administrasilain,
0 AS pemeliharaanlain,
0 AS retribusilain,
IFNULL(rek.meterai, 0) AS meterai,
ifnull(rek.total,0) - ifnull(rek.dendatunggakan,0) AS rekair,
IFNULL(rek.dendatunggakan, 0) AS denda,
IFNULL(rek.ppn, 0) AS ppn,
IFNULL(rek.total, 0) AS total,
NOW() AS waktuupdate
FROM
nonair rek
JOIN [bsbs].pelanggan pel ON pel.nosamb = rek.dibebankankepada
JOIN temp_dataawal_periode per ON per.periode = rek.periode
LEFT JOIN [bsbs].golongan gol ON gol.kodegol = rek.kodegol AND gol.aktif = 1
LEFT JOIN [bsbs].diameter dia ON dia.kodediameter = pel.kodediameter AND dia.aktif = 1
LEFT JOIN [bsbs].rayon ray ON ray.koderayon = rek.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = pel.kodekelurahan
,(SELECT @id := @lastid) AS id
WHERE rek.periode = @periode AND rek.dibebankankepada=@nosamb;