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
@idpdam AS `idpdam`,
per.`idpermohonan` AS `idpermohonan`,
spkp.`nomorspkp` AS `nomorspk`,
spkp.`tglpasang` AS `tanggalspk`,
spkp.`nomorsppb` AS `nomorsppb`,
spkp.`tglrab` AS `tanggalsppb`,
usr.iduser AS `iduser`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
spkp.`tglpasang` AS `waktuupdate`
FROM
`rab_sambung_kembali` spkp
JOIN __tmp_sambung_kembali per ON per.`nomor`=spkp.`nomorpermohonan`
LEFT JOIN __tmp_userloket usr ON usr.nama=spkp.`user`
WHERE spkp.`nomorspkp` IS NOT NULL AND `spkp`.`flaghapus`=0 and spkp.`tglpasang` is not null