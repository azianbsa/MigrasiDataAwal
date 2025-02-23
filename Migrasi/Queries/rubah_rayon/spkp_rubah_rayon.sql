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
per.nomor_spk AS nomorspk,
per.tanggal_spk AS tanggalspk,
NULL AS `nomorsppb`,
NULL AS `tanggalsppb`,
usr.iduser AS iduser,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
0 AS flagbatal,
NULL AS idalasanbatal,
per.`tanggal_spk` AS waktuupdate
FROM permohonan_rubah_rayon per
JOIN __tmp_permohonan_rubah_rayon p ON p.nomor=per.nomor
LEFT JOIN __tmp_userloket usr ON usr.nama = per.user_spk
WHERE per.flaghapus = 0 AND per.`flag_spk`=1 AND per.`nomor_spk` IS NOT NULL