DROP TEMPORARY TABLE IF EXISTS __tmp_pengaduan;
CREATE TEMPORARY TABLE __tmp_pengaduan AS
SELECT
@id:=@id+1 AS id,
p.nomor,
p.`kategori`
FROM pengaduan p
JOIN `__tmp_tipepermohonan` t ON t.`kodejenisnonair`=p.`kategori`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0 AND `flagpel`=1;

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
pp.id AS `idpermohonan`,
p.nomorspk,
p.tanggalspk,
NULL AS `nomorsppb`,
NULL AS `tanggalsppb`,
u.iduser,
NULL AS `fotobukti1`,
NULL AS `fotobukti2`,
NULL AS `fotobukti3`,
0 AS `flagbatal`,
NULL AS `idalasanbatal`,
COALESCE(p.tanggalspk,NOW()) AS `waktuupdate`
FROM
__tmp_pengaduan pp 
JOIN spk_pengaduan p ON p.nomorpengaduan=pp.nomor
LEFT JOIN __tmp_userloket u ON u.nama=p.user_spk
WHERE p.flaghapus=0 AND p.nomorspk IS NOT NULL AND p.tanggalspk IS NOT NULL