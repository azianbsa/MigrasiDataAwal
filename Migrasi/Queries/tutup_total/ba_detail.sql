DROP TABLE IF EXISTS __tmp_tutup_total;
CREATE TABLE __tmp_tutup_total AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_pemutusan_sementara`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

DROP TABLE IF EXISTS __tmp_merkmeter;
CREATE TABLE __tmp_merkmeter AS
SELECT
@id:=@id+1 AS id,
merk
FROM
merkmeter,
(SELECT @id:=0) AS id;

DROP TABLE IF EXISTS __tmp_warnasegel;
CREATE TABLE __tmp_warnasegel AS
SELECT
@id:=@id+1 AS id,
warna
FROM
`warnasegel`,
(SELECT
@id := 0) AS id;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Angka Meter' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
b.`angkameter` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_tutup_total p
LEFT JOIN `ba_pemutusan_sementara` b ON b.`nomorpermohonan`=p.`nomor`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Keterangan' AS `parameter`,
'string' AS `tipedata`,
b.memo AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_tutup_total p
LEFT JOIN `ba_pemutusan_sementara` b ON b.`nomorpermohonan`=p.`nomor`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Keterangan Putus' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_tutup_total p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Merk Meter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
m.id AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_tutup_total p
LEFT JOIN `ba_pemutusan_sementara` b ON b.`nomorpermohonan`=p.`nomor`
LEFT JOIN __tmp_merkmeter m ON m.merk=b.`merkmeter`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Seri Meter' AS `parameter`,
'string' AS `tipedata`,
b.`serimeter` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_tutup_total p
LEFT JOIN `ba_pemutusan_sementara` b ON b.`nomorpermohonan`=p.`nomor`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Seri Segel' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_tutup_total p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Status Rangkaian Meter' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_tutup_total p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Warna Segel' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
w.id AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_tutup_total p
LEFT JOIN `ba_pemutusan_sementara` b ON b.`nomorpermohonan`=p.`nomor`
LEFT JOIN __tmp_warnasegel w ON w.warna=b.`warnasegel`