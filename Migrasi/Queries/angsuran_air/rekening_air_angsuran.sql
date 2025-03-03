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
ang.id AS idangsuran,
ang.noangsuran AS noangsuran,
@jnsnonair AS idjenisnonair,
pel.id AS idpelangganair,
ang.nama AS nama,
ang.alamat AS alamat,
ang.notelp AS notelp,
ang.nohp AS nohp,
ang.waktudaftar AS waktudaftar,
ang.jumlahtermin AS jumlahtermin,
ang.jumlahangsuranpokok AS jumlahangsuranpokok,
ang.jumlahangsuranbunga AS jumlahangsuranbunga,
ang.jumlahuangmuka AS jumlahuangmuka,
ang.jumlah as total,
usr.iduser AS iduser,
ang.tglmulaitagih AS tglmulaitagihpertama,
ba.nomorba AS noberitaacara,
ba.tanggalba AS tglberitaacara,
ang.flagupload AS flagpublish,
ang.waktuupload AS waktupublish,
ang.flaglunas AS flaglunas,
ang.waktulunas AS waktulunas,
0 AS flaghapus,
COALESCE(ang.waktulunas,ang.waktuupload,ang.waktudaftar) AS waktuupdate
FROM daftarangsuran ang
JOIN pelanggan pel ON pel.nosamb = ang.dibebankankepada
LEFT JOIN __tmp_userloket usr ON usr.nama = ang.userdaftar
LEFT JOIN ba_angsuran ba ON ba.noangsuran=ang.noangsuran and ba.flaghapus=0
WHERE ang.keperluan='JNS-36'