DROP TEMPORARY TABLE IF EXISTS temp_dataawal_loket;
CREATE TEMPORARY TABLE temp_dataawal_loket AS
SELECT
 @id := @id + 1 AS idloket,
 kodeloket
FROM
 [loket].loket,
 (SELECT @id := 0) AS id
 ORDER BY kodeloket;

DROP TEMPORARY TABLE IF EXISTS temp_dataawal_user;
CREATE TEMPORARY TABLE temp_dataawal_user AS
SELECT
@id := @id + 1 AS iduser,
ul.nama
FROM
 [loket].userloket ul
 ,(SELECT @id := 0) AS id
 ORDER BY nama;

SELECT
 na.id,
 @idpdam,
 NULL AS idangsuran,
 jns.id AS idnonair,
 pel.id AS idpelangganair,
 NULL AS idpelangganlimbah,
 NULL AS idpelangganlltt,
 na.periode AS kodeperiode,
 na.termin AS termin,
 IF(na.flagbatal=0,na.flaglunas,0) AS statustransaksi,
 na.urutan AS nomortransaksi,
 na.waktubayar AS waktutransaksi,
 YEAR(na.waktubayar) AS tahuntransaksi,
 us.iduser AS iduser,
 lo.idloket AS idloket,
 NULL AS idkolektiftransaksi,
 NULL AS idalasanbatal,
 na.ketjenis AS keterangan,
 ryn.id AS idrayon,
 NULL AS idkelurahan,
 gol.id AS idgolongan,
 NULL AS iddiameter,
 na.tglmulaitagih AS tglmulaitagih,
 IFNULL(na.biayabahan,0) AS biayabahan,
 IFNULL(na.biayapasang,0) AS biayapasang,
 0 AS ujl,
 IFNULL(na.ppn,0) AS ppn,
 IFNULL(na.pendaftaran,0) AS pendaftaran,
 IFNULL(na.administrasi,0) AS administrasi,
 IFNULL(na.meterai,0) AS meterai,
 IFNULL(na.lainnya,0) AS lainnya,
 IFNULL(na.total,0) AS total,
 IFNULL(na.waktuupdate,NOW()) AS waktuupdate
FROM
 nonair na
 LEFT JOIN pelanggan pel ON pel.nosamb = na.dibebankankepada
 LEFT JOIN [loket].jenisnonair jns ON jns.jenis = na.jenis
 LEFT JOIN rayon ryn ON ryn.koderayon = na.koderayon
 LEFT JOIN golongan gol ON gol.kodegol = na.kodegol AND gol.aktif = 1
 LEFT JOIN temp_dataawal_user us ON us.nama = na.kasir
 LEFT JOIN temp_dataawal_loket lo ON lo.kodeloket = na.loketbayar
 WHERE na.flaghapus = 0 AND na.flagangsur = 1 AND (na.periode = @periode OR na.periode IS NULL OR na.periode = '')