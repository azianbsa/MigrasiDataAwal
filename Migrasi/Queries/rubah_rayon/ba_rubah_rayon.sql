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
@idpdam AS idpdam,
p.idpermohonan AS idpermohonan,
per.nomor_ba AS nomorba,
per.tanggal_ba AS tanggalba,
usr.iduser AS iduser,
NULL AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
NULL AS kategoriputus,
0 AS flagbatal,
NULL AS idalasanbatal,
IF(per.`tanggal_verifikasi` IS NOT NULL,1,0) AS flag_dari_verifikasi,
NULL AS statusberitaacara,
per.`tanggal_ba` AS waktuupdate
FROM permohonan_rubah_rayon per
JOIN __tmp_permohonan_rubah_rayon p ON p.nomor=per.nomor
LEFT JOIN __tmp_userloket usr ON usr.nama = per.user_ba
WHERE per.flaghapus = 0 AND per.`flag_ba`=1 AND per.`nomor_ba` IS NOT NULL