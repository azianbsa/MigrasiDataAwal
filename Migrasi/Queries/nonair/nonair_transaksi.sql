DROP TEMPORARY TABLE IF EXISTS __tmp_loket;
CREATE TEMPORARY TABLE __tmp_loket AS
SELECT
@id := @id + 1 AS idloket,
kodeloket,
loket
FROM
loket,
(SELECT @id := 0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_userloket;
CREATE TEMPORARY TABLE __tmp_userloket AS
SELECT
@idpdam,
@id := @id + 1 AS iduser,
a.nama,
a.namauser
FROM (
SELECT nama,namauser,`passworduser`,alamat,aktif FROM [bacameter].`userakses`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,aktif FROM [bsbs].`userakses`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userloket`
UNION
SELECT nama,namauser,`passworduser`,NULL AS alamat,flagaktif AS aktif FROM `userbshl`
) a,
(SELECT @id := 0) AS id
GROUP BY a.namauser;

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
[table] na
LEFT JOIN __tmp_userloket us ON us.nama=na.kasir
LEFT JOIN __tmp_loket lo ON lo.kodeloket=na.loketbayar
WHERE jenis<>'JNS-16' AND flagangsur=0 AND flaglunas=1 AND flagbatal=0 AND (na.periode = @periode OR na.periode IS NULL OR na.periode = '')