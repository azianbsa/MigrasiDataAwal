DROP TEMPORARY TABLE IF EXISTS __tmp_rubah_tarif;
CREATE TEMPORARY TABLE __tmp_rubah_tarif AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_rubah_gol`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

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
t.`id` AS `idpermohonan`,
p.`nospk_pengecekan` AS `nomorspk`,
p.`tglspk_pengecekan` AS `tanggalspk`,
u.`iduser` AS `iduser`,
1 AS `flagsurvey`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
p.`tglspk_pengecekan` AS `waktuupdate`
FROM __tmp_rubah_tarif t
JOIN `permohonan_rubah_gol` p ON p.nomor=t.nomor
LEFT JOIN __tmp_userloket u ON u.nama = SUBSTRING_INDEX(p.urutannonair,'.RUBAH_GOL.',1)
WHERE p.`flag_spk_pengecekan`=1