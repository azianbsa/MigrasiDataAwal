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

DROP TEMPORARY TABLE IF EXISTS __tmp_tutup_total;
CREATE TEMPORARY TABLE __tmp_tutup_total AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_pemutusan_sementara`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
spk.`nomorspk` AS `nomorspk`,
spk.`tanggalspk` AS `tanggalspk`,
NULL AS `nomorsppb`,
NULL AS `tanggalsppb`,
usr.iduser AS `iduser`,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
spk.`tanggalspk` AS `waktuupdate`
FROM __tmp_tutup_total p
JOIN `spk_pemutusan_sementara` spk ON spk.nomorpermohonan=p.nomor
LEFT JOIN __tmp_userloket usr ON usr.nama=spk.user
WHERE spk.flaghapus=0;