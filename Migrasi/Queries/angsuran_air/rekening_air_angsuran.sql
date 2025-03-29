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
a.id AS idangsuran,
a.noangsuran AS noangsuran,
@jnsnonair AS idjenisnonair,
p.id AS idpelangganair,
a.nama AS nama,
a.alamat AS alamat,
a.notelp AS notelp,
a.nohp AS nohp,
a.nama AS `namapemohon`,
LEFT(a.alamat,150) AS `alamatpemohon`,
a.notelp AS `notelppemohon`,
a.nohp AS `nohppemohon`,
a.waktudaftar AS waktudaftar,
a.jumlahtermin AS jumlahtermin,
a.jumlahangsuranpokok AS jumlahangsuranpokok,
a.jumlahangsuranbunga AS jumlahangsuranbunga,
a.jumlahuangmuka AS jumlahuangmuka,
a.jumlah as total,
usr.iduser AS iduser,
a.tglmulaitagih AS tglmulaitagihpertama,
ba.nomorba AS noberitaacara,
ba.tanggalba AS tglberitaacara,
a.flagupload AS flagpublish,
a.waktuupload AS waktupublish,
a.flaglunas AS flaglunas,
a.waktulunas AS waktulunas,
0 AS flaghapus,
COALESCE(a.waktulunas,a.waktuupload,a.waktudaftar) AS waktuupdate
FROM daftarangsuran a
JOIN pelanggan p ON p.nosamb=a.dibebankankepada
LEFT JOIN __tmp_userloket usr ON usr.nama=a.userdaftar
LEFT JOIN ba_angsuran ba ON ba.noangsuran=a.noangsuran AND ba.flaghapus=0
WHERE a.keperluan='JNS-36'
AND DATE(a.waktuupload)<=@cutoff