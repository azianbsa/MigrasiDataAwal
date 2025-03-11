DROP TABLE IF EXISTS __tmp_pengaduan;
CREATE TABLE __tmp_pengaduan AS
SELECT
@id:=@id+1 AS id,
p.nomor,
p.`kategori`
FROM pengaduan p
JOIN `__tmp_tipepermohonan` t ON t.`kodejenisnonair`=p.`kategori`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0 AND `flagpel`=1;

DROP TABLE IF EXISTS __tmp_bagian;
CREATE TABLE __tmp_bagian AS
SELECT @id:=@id+1 AS id,kodebagian,namabagian 
FROM `pengaduan_bagian`,(SELECT @id:=0) AS id;

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
'JNS-29',
'JNS-113',
'JNS-100',
'JNS-101',
'JNS-102',
'JNS-103',
'JNS-31',
'JNS-104',
'JNS-106',
'JNS-108',
'JNS-119',
'JNS-109',
'JNS-110',
'JNS-111',
'JNS-112',
'JNS-114',
'JNS-117',
'JNS-118',
'JNS-121',
'JNS-123',
'JNS-124',
'JNS-115',
'JNS-19',
'JNS-126',
'JNS-127',
'JNS-128',
'JNS-129',
'JNS-130',
'JNS-131',
'JNS-132',
'JNS-133',
'JNS-134',
'JNS-135',
'JNS-136',
'JNS-137',
'JNS-138',
'JNS-142',
'JNS-143',
'JNS-146',
'JNS-148',
'JNS-150'
)
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Ditagih Setelah' AS `parameter`,
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
'JNS-29',
'JNS-113',
'JNS-100',
'JNS-101',
'JNS-102',
'JNS-103',
'JNS-31',
'JNS-104',
'JNS-106',
'JNS-108',
'JNS-119',
'JNS-109',
'JNS-110',
'JNS-111',
'JNS-112',
'JNS-114',
'JNS-117',
'JNS-118',
'JNS-121',
'JNS-123',
'JNS-124',
'JNS-115',
'JNS-19',
'JNS-126',
'JNS-127',
'JNS-128',
'JNS-129',
'JNS-130',
'JNS-131',
'JNS-132',
'JNS-133',
'JNS-134',
'JNS-135',
'JNS-136',
'JNS-137',
'JNS-138',
'JNS-142',
'JNS-143',
'JNS-146',
'JNS-148',
'JNS-150'
)