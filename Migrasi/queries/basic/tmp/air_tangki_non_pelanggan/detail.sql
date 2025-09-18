DROP TABLE IF EXISTS __tmp_pengaduan;
CREATE TABLE __tmp_pengaduan AS
SELECT
@id:=@id+1 AS id,
p.nomor,
p.`kategori`
FROM pengaduan p
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0 AND p.kategori='JNS-9' AND `flagpel`=0;

DROP TEMPORARY TABLE IF EXISTS __tmp_bagian;
CREATE TEMPORARY TABLE __tmp_bagian AS
SELECT @id:=@id+1 AS id,kodebagian,namabagian 
FROM `pengaduan_bagian`,(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_tat;
CREATE TEMPORARY TABLE __tmp_tat AS
SELECT
@id:=@id+1 AS id,
kodetarif
FROM `tarif_airtangki`
,(SELECT @id:=0) AS id;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Administrasi (Rp)' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
pp.`biayaadministrasi` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN pengaduan b ON b.`nomor`=p.`nomor`
LEFT JOIN `pengaduan_airtangki` pp ON pp.nomor_pengaduan=p.`nomor`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Biaya Air (Rp)' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
pp.`biayaair` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN pengaduan b ON b.`nomor`=p.`nomor`
LEFT JOIN `pengaduan_airtangki` pp ON pp.nomor_pengaduan=p.`nomor`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Ditagih Setelah' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Jarak (Km)' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
pp.`jarak` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
LEFT JOIN `pengaduan_airtangki` pp ON pp.nomor_pengaduan=p.`nomor`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Jumlah (m3)' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
pp.`jumlah_tangki` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
LEFT JOIN `pengaduan_airtangki` pp ON pp.nomor_pengaduan=p.`nomor`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Jumlah Armada' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
pp.`jumlah_diantar` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
LEFT JOIN `pengaduan_airtangki` pp ON pp.nomor_pengaduan=p.`nomor`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Operasional (Rp)' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
pp.`biayaoperasional` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
LEFT JOIN `pengaduan_airtangki` pp ON pp.nomor_pengaduan=p.`nomor`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Ppn' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
pp.`biayappn` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
LEFT JOIN `pengaduan_airtangki` pp ON pp.nomor_pengaduan=p.`nomor`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Tanggal Pemesanan' AS `parameter`,
'date' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
b.`tglditerima` AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Tarif Air Tangki' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
t.id AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
LEFT JOIN `pengaduan_airtangki` pp ON pp.nomor_pengaduan=p.`nomor`
LEFT JOIN __tmp_tat t ON t.kodetarif=pp.`kodetarif`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Transport (Rp)' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
pp.biayatransport AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
LEFT JOIN `pengaduan_airtangki` pp ON pp.nomor_pengaduan=p.`nomor`