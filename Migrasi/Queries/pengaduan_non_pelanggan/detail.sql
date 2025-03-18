DROP TABLE IF EXISTS __tmp_pengaduan;
CREATE TABLE __tmp_pengaduan AS
SELECT
@id:=@id+1 AS id,
p.nomor,
p.`kategori`
FROM pengaduan p
JOIN `__tmp_tipepermohonan` t ON t.`kodejenisnonair`=p.`kategori`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0 AND `flagpel`=0;

DROP TABLE IF EXISTS __tmp_bagian;
CREATE TABLE __tmp_bagian AS
SELECT @id:=@id+1 AS id,kodebagian,namabagian 
FROM `pengaduan_bagian`,(SELECT @id:=0) AS id;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Alamat Pelapor' AS `parameter`,
'string' AS `tipedata`,
b.`alamatpelapor` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN pengaduan b ON b.`nomor`=p.`nomor`
LEFT JOIN __tmp_bagian u ON u.`kodebagian`=b.`diteruskankebagian`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-108',
'JNS-119',
'JNS-109',
'JNS-137',
'JNS-142',
'JNS-143'
)
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Bagian' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
u.`id` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN pengaduan b ON b.`nomor`=p.`nomor`
LEFT JOIN __tmp_bagian u ON u.`kodebagian`=b.`diteruskankebagian`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-108',
'JNS-119',
'JNS-109',
'JNS-137',
'JNS-142',
'JNS-143'
)
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'HP Pelapor' AS `parameter`,
'string' AS `tipedata`,
b.`nohppelapor` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-108',
'JNS-119',
'JNS-109',
'JNS-137',
'JNS-142',
'JNS-143'
)
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Nama Pelapor' AS `parameter`,
'string' AS `tipedata`,
b.`namapelapor` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-108',
'JNS-119',
'JNS-109',
'JNS-137',
'JNS-142',
'JNS-143'
)
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Telp. Pelapor' AS `parameter`,
'string' AS `tipedata`,
b.`ditagih_setelah` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-108',
'JNS-119',
'JNS-109',
'JNS-137',
'JNS-142',
'JNS-143'
)