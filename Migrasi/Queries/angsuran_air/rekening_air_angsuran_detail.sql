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

DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
aktif
FROM
golongan,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_diameter;
CREATE TEMPORARY TABLE __tmp_diameter AS
SELECT
@id:=@id+1 AS id,
kodediameter,
aktif
FROM
diameter,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_angsuranair;
CREATE TEMPORARY TABLE __tmp_angsuranair AS
SELECT
b.id,
b.noangsuran,
CONCAT(a.periode,'.',a.nomor) AS kode,
b.flaglunas
FROM detailangsuran a
JOIN daftarangsuran b ON b.noangsuran=a.noangsuran
WHERE b.keperluan='JNS-36'
GROUP BY a.periode,a.nomor;

SELECT
d.id AS id,
@idpdam,
a.id AS idangsuran,
per.idperiode AS idperiode,
p.id AS idpelangganair,
d.termin AS termin,
d.flaglunas AS statustransaksi,
d.noangsuran AS nomortransaksi,
d.waktubayar AS waktutransaksi,
YEAR(d.waktubayar) AS tahuntransaksi,
usr.iduser AS iduser,
lo.idloket AS idloket,
NULL AS idkolektiftransaksi,
NULL AS idalasanbatal,
d.ketjenis AS keterangan,
ray.id AS idrayon,
kel.id AS idkelurahan,
gol.id AS idgolongan,
dia.id AS iddiameter,
d.tglmulaitagih AS tglmulaitagih,
IFNULL(d.biayapemakaian, 0) AS biayapemakaian,
IFNULL(d.administrasi, 0) AS administrasi,
IFNULL(d.pemeliharaan, 0) AS pemeliharaan,
IFNULL(d.retribusi, 0) AS retribusi,
0 AS pelayanan,
0 AS airlimbah,
0 AS dendapakai0,
0 AS administrasilain,
0 AS pemeliharaanlain,
0 AS retribusilain,
IFNULL(d.meterai, 0) AS meterai,
ifnull(d.jumlah,0) - ifnull(d.dendatunggakan,0) AS rekair,
IFNULL(d.dendatunggakan, 0) AS denda,
IFNULL(d.ppn, 0) AS ppn,
IFNULL(d.jumlah, 0) AS total,
NOW() AS waktuupdate
FROM
detailangsuran d
JOIN pelanggan p ON p.nosamb=d.dibebankankepada
JOIN __tmp_angsuranair a on a.kode=CONCAT(d.periode,'.',d.dibebankankepada)
JOIN __tmp_periode per ON per.periode=d.periode
LEFT JOIN __tmp_userloket usr ON usr.nama=d.kasir
LEFT JOIN __tmp_loket lo ON lo.kodeloket=d.loketbayar
LEFT JOIN __tmp_golongan gol ON gol.kodegol=p.kodegol AND gol.aktif=1
LEFT JOIN __tmp_diameter dia ON dia.kodediameter=p.kodediameter AND dia.aktif=1
LEFT JOIN [bsbs].rayon ray ON ray.koderayon=p.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan=p.kodekelurahan