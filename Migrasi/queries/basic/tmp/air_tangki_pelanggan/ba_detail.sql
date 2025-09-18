DROP TABLE IF EXISTS __tmp_pengaduan;
CREATE TABLE __tmp_pengaduan AS
SELECT
@id:=@id+1 AS id,
p.nomor,
p.`kategori`
FROM pengaduan p
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0 AND p.kategori='JNS-9' AND `flagpel`=0;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Diantar Hari Ini' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
pp.`jumlah_diantar` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
LEFT JOIN `pengaduan_airtangki` pp ON pp.nomor_pengaduan=p.`nomor`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Diantar Sebelumnya' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Jumlah Sisa' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
pp.`jumlah_sisa` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
LEFT JOIN `pengaduan_airtangki` pp ON pp.nomor_pengaduan=p.`nomor`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Jumlah Tangki' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
pp.`jumlah_tangki` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
LEFT JOIN `pengaduan_airtangki` pp ON pp.nomor_pengaduan=p.`nomor`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Keterangan' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Status Pemesanan' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Waktu Antar Mulai' AS `parameter`,
'date' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
b.`tglpekerjaan1` AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Waktu Antar Selesai' AS `parameter`,
'date' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
b.`tglpekerjaan2` AS `valuedate`,
NULL AS `valuebool`,
b.`tgldiselesaikan` AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `spk_pengaduan` b ON b.`nomorpengaduan`=p.`nomor`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tgldiselesaikan` IS NOT NULL