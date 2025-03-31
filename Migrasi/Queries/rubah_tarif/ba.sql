﻿DROP TEMPORARY TABLE IF EXISTS __tmp_rubah_tarif;
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
@idpdam AS idpdam,
t.id AS idpermohonan,
p.noba_pengecekan AS nomorba,
p.tglba_pengecekan AS tanggalba,
u.iduser AS iduser,
NULL AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
NULL AS fotobukti1,
NULL AS fotobukti2,
NULL AS fotobukti3,
NULL AS `fotobukti4`,
NULL AS `fotobukti5`,
NULL AS `fotobukti6`,
NULL AS kategoriputus,
0 AS flagbatal,
NULL AS idalasanbatal,
1 AS flag_dari_verifikasi,
NULL AS statusberitaacara,
NOW() AS waktuupdate
FROM __tmp_rubah_tarif t
JOIN permohonan_rubah_gol p on p.nomor=t.nomor
LEFT JOIN __tmp_userloket u ON u.nama = SUBSTRING_INDEX(p.urutannonair,'.RUBAH_GOL.',1)
WHERE p.flag_ba_pengecekan=1