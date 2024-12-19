DROP TEMPORARY TABLE IF EXISTS temp_dataawal_loket;
CREATE TEMPORARY TABLE temp_dataawal_loket AS
SELECT
 @id := @id + 1 AS idloket,
 kodeloket
FROM
 loket,
 (SELECT @id := 0) AS id
 ORDER BY kodeloket;

DROP TEMPORARY TABLE IF EXISTS temp_dataawal_user;
CREATE TEMPORARY TABLE temp_dataawal_user AS
SELECT
@id := @id + 1 AS iduser,
ul.nama
FROM
 userloket ul
 ,(SELECT @id := 0) AS id
 ORDER BY nama;

SELECT
 @idpdam,
 id AS idnonair,
 nomor AS nomortransaksi,
 1 AS statustransaksi,
 waktubayar AS waktutransaksi,
 YEAR(waktubayar) AS tahuntransaksi,
 us.iduser AS iduser,
 lo.idloket AS idloket,
 NULL AS idkolektiftransaksi,
 NULL AS idalasanbatal,
 keterangan AS keterangan,
 waktuupdate AS waktuupdate
FROM
 nonair na
 LEFT JOIN temp_dataawal_user us ON us.nama=na.kasir
 LEFT JOIN temp_dataawal_loket lo ON lo.kodeloket=na.loketbayar
 WHERE flaghapus=0 AND flagangsur=0 AND flaglunas=1 AND flagbatal=0 AND (na.periode = @periode OR na.periode IS NULL OR na.periode = '')