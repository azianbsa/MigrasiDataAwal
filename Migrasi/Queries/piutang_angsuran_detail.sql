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
 SUBSTRING_INDEX(rek.kode, '.', -1) AS termin,
 IF(rek.flagbatal=0,rek.flaglunas,0) AS statustransaksi,
 rek.noangsuran AS nomortransaksi,
 rek.tglbayar AS waktutransaksi,
 YEAR(rek.tglbayar) AS tahuntransaksi,
 NULL AS iduser,
 NULL AS idloket,
 NULL AS idkolektiftransaksi,
 NULL AS idalasanbatal,
 NULL AS keterangan,
 ray.id AS idrayon,
 NULL AS idkelurahan,
 gol.id AS idgolongan,
 dia.id AS iddiameter,
 DATE_FORMAT(DATE_ADD(STR_TO_DATE(CONCAT(rek.periode,'01'), '%Y%m%d'), INTERVAL 1 MONTH), '%Y-%m-01') AS tglmulaitagih,
 IFNULL(rek.biayapemakaian, 0) AS biayapemakaian,
 IFNULL(rek.administrasi, 0) AS administrasi,
 IFNULL(rek.pemeliharaan, 0) AS pemeliharaan,
 IFNULL(rek.retribusi, 0) AS retribusi,
 IFNULL(rek.pelayanan, 0) AS pelayanan,
 IFNULL(rek.airlimbah, 0) AS airlimbah,
 IFNULL(rek.dendapakai0, 0) AS dendapakai0,
 IFNULL(rek.administrasilain, 0) AS administrasilain,
 IFNULL(rek.pemeliharaanlain, 0) AS pemeliharaanlain,
 IFNULL(rek.retribusilain, 0) AS retribusilain,
 IFNULL(rek.meterai, 0) AS meterai,
 IFNULL(rek.rekair, 0) AS rekair,
 IFNULL(rek.dendatunggakan, 0) AS denda,
 IFNULL(rek.ppn, 0) AS ppn,
 IFNULL(rek.total, 0) AS total,
 NOW() AS waktuupdate
FROM
 piutang rek
 JOIN [bsbs].pelanggan pel ON pel.nosamb = rek.nosamb
 JOIN temp_dataawal_periode per ON per.periode = rek.periode
 LEFT JOIN [bsbs].golongan gol ON gol.kodegol = rek.kodegol AND gol.aktif = 1
 LEFT JOIN [bsbs].diameter dia ON dia.kodediameter = rek.kodediameter AND dia.aktif = 1
 LEFT JOIN [bsbs].rayon ray ON ray.koderayon = rek.koderayon
 -- LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = rek.kodekelurahan
 ,(SELECT @id := @lastid) AS id
 WHERE rek.kode <> CONCAT(rek.periode, '.', rek.nosamb);