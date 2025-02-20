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
usr.iduser AS iduser,
lo.idloket AS idloket,
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
JOIN pelanggan pel ON pel.nosamb = ang.dibebankankepada
JOIN __tmp_periode per ON per.periode = ang.periode
LEFT JOIN __tmp_userloket usr ON usr.nama = ang.kasir
LEFT JOIN __tmp_loket lo ON lo.kodeloket = ang.loketbayar
LEFT JOIN __tmp_golongan gol ON gol.kodegol = pel.kodegol AND gol.aktif = 1
LEFT JOIN __tmp_diameter dia ON dia.kodediameter = pel.kodediameter AND dia.aktif = 1
LEFT JOIN [bsbs].rayon ray ON ray.koderayon = pel.koderayon
LEFT JOIN [bsbs].kelurahan kel ON kel.kodekelurahan = pel.kodekelurahan