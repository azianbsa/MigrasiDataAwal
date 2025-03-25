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

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Angka Meter Baru' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-109',
'JNS-142'
) AND b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Diameter Baru' AS `parameter`,
'int' AS `tipedata`,
LEFT(b.`uraianpenyelesaian`,250) AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-109',
'JNS-142'
) AND b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Keterangan Lapangan' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-109',
'JNS-142'
) AND b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Merk Meter Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-109',
'JNS-142'
) AND b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Meteran Diganti' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-109',
'JNS-142'
) AND b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'No Segel Baru' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-109',
'JNS-142'
) AND b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Serimeter Baru' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
b.`tglpekerjaan2` AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-109',
'JNS-142'
) AND b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Stan Angkat' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
b.`tglpekerjaan1` AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-109',
'JNS-142'
) AND b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Tanggal Pengerjaan Akhir' AS `parameter`,
'date' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
b.`tglpekerjaan1` AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-109',
'JNS-142'
) AND b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Tanggal Pengerjaan Awal' AS `parameter`,
'date' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
b.`tglpekerjaan1` AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE p.`kategori` IN (
'JNS-113',
'JNS-109',
'JNS-142'
) AND b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL