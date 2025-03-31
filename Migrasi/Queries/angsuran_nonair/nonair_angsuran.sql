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
@idpdam,
na.`idangsuran` AS idangsuran,
na.noangsuran1 AS noangsuran,
na.`id` AS idnonair,
jns.idjenisnonair AS idjenisnonair,
pel.id AS idpelangganair,
NULL AS idpelangganlimbah,
NULL AS idpelangganlltt,
na.nama AS nama,
na.alamat AS alamat,
na.notelp AS notelp,
na.nohp AS nohp,
na.nama AS `namapemohon`,
na.alamat AS `alamatpemohon`,
na.notelp AS `notelppemohon`,
na.nohp AS `nohppemohon`,
d.waktudaftar AS waktudaftar,
na.`jumlahtermin` AS jumlahtermin,
d.`jumlahangsuranpokok` AS jumlahangsuranpokok,
d.`jumlahangsuranbunga` AS jumlahangsuranbunga,
d.`jumlahuangmuka` AS jumlahuangmuka,
d.`jumlah` AS total,
us.iduser AS iduser,
d.`tglmulaitagih` AS tglmulaitagihpertama,
na.nolpp AS noberitaacara,
NULL AS tglberitaacara,
d.flagupload AS flagpublish,
d.`waktuupload` AS waktupublish,
d.`flaglunas` AS flaglunas,
d.`waktulunas` AS waktulunas,
0 AS flaghapus,
NOW() AS waktuupdate
FROM __tmp_nonair na
JOIN `daftarangsuran` d ON d.`id`=na.`idangsuran`
LEFT JOIN pelanggan pel ON pel.nosamb=na.dibebankankepada
LEFT JOIN __tmp_jenisnonair jns ON jns.kodejenisnonair=na.jenis
LEFT JOIN __tmp_userloket us ON us.nama=d.`userdaftar`