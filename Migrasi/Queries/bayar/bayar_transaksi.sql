DROP TEMPORARY TABLE IF EXISTS __tmp_periode;
CREATE TEMPORARY TABLE __tmp_periode AS
SELECT
@id:=@id+1 AS idperiode,
periode
FROM
[bsbs].periode
,(SELECT @id:=0) AS id
ORDER BY periode;

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

DROP TEMPORARY TABLE IF EXISTS __tmp_loket;
CREATE TEMPORARY TABLE __tmp_loket AS
SELECT
@idloket := @idloket + 1 AS idloket,
kodeloket,
loket
FROM loket
,(SELECT @idloket := 0) AS idloket
ORDER BY kodeloket;

SELECT
@idpdam,
pel.id AS idpelangganair,
per.idperiode AS idperiode,
rek.nolpp AS nomortransaksi,
IF(rek.flagbatal=0,1,0) AS statustransaksi,
rek.tglbayar AS waktutransaksi,
YEAR(rek.tglbayar) AS tahuntransaksi,
usr.iduser AS iduser,
lo.idloket AS idloket,
NULL AS idkolektiftransaksi,
6 AS idalasanbatal,
NULL AS keterangan,
NOW() AS waktuupdate
FROM
[table] rek
JOIN pelanggan pel ON pel.nosamb=rek.nosamb
JOIN __tmp_periode per ON per.periode=rek.periode
LEFT JOIN __tmp_userloket usr ON usr.nama=rek.kasir
LEFT JOIN __tmp_loket lo ON lo.kodeloket=rek.loketbayar
WHERE rek.periode=@periode
AND rek.flagangsur=0
AND rek.kode NOT LIKE '%\_'
AND DATE(rek.tglbayar)=@cutoff