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

DROP TEMPORARY TABLE IF EXISTS __tmp_nonair;
CREATE TEMPORARY TABLE __tmp_nonair AS
SELECT
@id:=@id+1 AS id,
urutan
FROM [table]
,(SELECT @id:=@lastid) AS id
WHERE flagangsur=0
AND jenis<>'JNS-38'
AND DATE(COALESCE(waktuinput,waktuupdate))=@cutoff;

SELECT
@idpdam,
n.id AS idnonair,
na.urutan AS nomortransaksi,
0 AS statustransaksi,
na.waktubayar AS waktutransaksi,
YEAR(na.waktubayar) AS tahuntransaksi,
us.iduser AS iduser,
lo.idloket AS idloket,
NULL AS idkolektiftransaksi,
NULL AS idalasanbatal,
na.keterangan AS keterangan,
na.waktuupdate AS waktuupdate
FROM
__tmp_nonair n
JOIN [table] na ON na.urutan=n.urutan
LEFT JOIN __tmp_userloket us ON us.nama=na.kasir
LEFT JOIN __tmp_loket lo ON lo.kodeloket=na.loketbayar
WHERE na.flagbatal=1