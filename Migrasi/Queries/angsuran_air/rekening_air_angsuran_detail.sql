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
ang.id AS id,
@idpdam,
aa.idangsuran AS idangsuran,
per.idperiode AS idperiode,
pel.id AS idpelangganair,
ang.termin AS termin,
ang.flaglunas AS statustransaksi,
ang.noangsuran AS nomortransaksi,
ang.waktubayar AS waktutransaksi,
YEAR(ang.waktubayar) AS tahuntransaksi,
NULL AS iduser,
NULL AS idloket,
NULL AS idkolektiftransaksi,
NULL AS idalasanbatal,
ang.ketjenis AS keterangan,
ray.id AS idrayon,
kel.id AS idkelurahan,
gol.id AS idgolongan,
dia.id AS iddiameter,
ang.tglmulaitagih AS tglmulaitagih,
IFNULL(ang.biayapemakaian, 0) AS biayapemakaian,
IFNULL(ang.administrasi, 0) AS administrasi,
IFNULL(ang.pemeliharaan, 0) AS pemeliharaan,
IFNULL(ang.retribusi, 0) AS retribusi,
0 AS pelayanan,
0 AS airlimbah,
0 AS dendapakai0,
0 AS administrasilain,
0 AS pemeliharaanlain,
0 AS retribusilain,
IFNULL(ang.meterai, 0) AS meterai,
ifnull(ang.jumlah,0) - ifnull(ang.dendatunggakan,0) AS rekair,
IFNULL(ang.dendatunggakan, 0) AS denda,
IFNULL(ang.ppn, 0) AS ppn,
IFNULL(ang.jumlah, 0) AS total,
NOW() AS waktuupdate
FROM
detailangsuran ang
JOIN __tmp_angsuranair aa on aa.kode = concat(ang.periode,'.',ang.dibebankankepada)
JOIN [bsbs].pelanggan pel ON pel.nosamb = ang.dibebankankepada
JOIN temp_dataawal_periode per ON per.periode = ang.periode
LEFT JOIN [bsbs].golongan gol ON gol.kodegol = pel.kodegol AND gol.aktif = 1
LEFT JOIN [bsbs].diameter dia ON dia.kodediameter = pel.kodediameter AND dia.aktif = 1
LEFT JOIN [bsbs].rayon ray ON ray.koderayon = pel.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = pel.kodekelurahan