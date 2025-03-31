DROP TEMPORARY TABLE IF EXISTS __tmp_loket;
CREATE TEMPORARY TABLE __tmp_loket AS
SELECT
@idloket := @idloket + 1 AS idloket,
kodeloket,
loket
FROM loket
,(SELECT @idloket := 0) AS idloket
ORDER BY kodeloket;

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

DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
aktif
FROM
golongan,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_nonair;
CREATE TEMPORARY TABLE __tmp_nonair AS
SELECT 
a.id AS idangsuran,
a.jumlahtermin,
a.noangsuran AS noangsuran1,
b.*
FROM `daftarangsuran` a
LEFT JOIN nonair b ON b.`urutan`=a.`urutan_nonair`
WHERE a.`keperluan`<>'JNS-36'
AND b.jenis<>'JNS-38'
AND (DATE(a.waktuupload)<=@cutoff OR a.flagupload=0);

SELECT
d.id,
@idpdam,
na.`idangsuran` AS idangsuran,
na.`id` AS idnonair,
pel.id AS idpelangganair,
NULL AS idpelangganlimbah,
NULL AS idpelangganlltt,
IF(d.`periode`='',NULL,d.`periode`) AS kodeperiode,
d.`termin` AS termin,
d.`flaglunas` AS statustransaksi,
d.`noangsuran` AS nomortransaksi,
IF(d.`waktubayar`='0000-00-00 00:00:00',NULL,d.`waktubayar`) AS waktutransaksi,
YEAR(d.waktubayar) AS tahuntransaksi,
us.iduser AS iduser,
lo.idloket AS idloket,
NULL AS idkolektiftransaksi,
NULL AS idalasanbatal,
d.ketjenis AS keterangan,
ryn.id AS idrayon,
NULL AS idkelurahan,
gol.id AS idgolongan,
NULL AS iddiameter,
d.tglmulaitagih AS tglmulaitagih,
IFNULL(d.biayabahan,0) AS biayabahan,
IFNULL(d.biayapasang,0) AS biayapasang,
0 AS ujl,
IFNULL(d.ppn,0) AS ppn,
IFNULL(d.pendaftaran,0) AS pendaftaran,
IFNULL(d.administrasi,0) AS administrasi,
IFNULL(d.meterai,0) AS meterai,
IFNULL(d.lainnya,0) AS lainnya,
IFNULL(d.jumlah,0) AS total,
NOW() AS waktuupdate
FROM __tmp_nonair na
JOIN detailangsuran d ON d.`noangsuran`=na.`noangsuran1`
LEFT JOIN pelanggan pel ON pel.nosamb=na.dibebankankepada
LEFT JOIN __tmp_golongan gol ON gol.kodegol=na.kodegol AND gol.aktif=1
LEFT JOIN __tmp_userloket us ON us.nama=na.kasir
LEFT JOIN __tmp_loket lo ON lo.kodeloket=na.loketbayar
LEFT JOIN [bsbs].rayon ryn ON ryn.koderayon=na.koderayon