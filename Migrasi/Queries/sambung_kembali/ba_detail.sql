DROP TEMPORARY TABLE IF EXISTS __tmp_merkmeter;
CREATE TEMPORARY TABLE __tmp_merkmeter AS
SELECT
@id:=@id+1 AS id,
merk
FROM
merkmeter,
(SELECT @id:=0) AS id;

DROP TABLE IF EXISTS __tmp_sambung_kembali;
CREATE TABLE __tmp_sambung_kembali AS
SELECT
@id := @id+1 AS ID,
p.nomor
FROM permohonan_sambung_kembali P
,(SELECT @id := @lastid) AS id
WHERE p.flaghapus=0;

SELECT
@idpdam AS `idpdam`,
p.id AS `idpermohonan`,
'Keterangan' AS `parameter`,
'string' AS `tipedata`,
ba.`memo` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM `__tmp_sambung_kembali` p
JOIN `ba_sambungkembali` ba ON ba.nomorpermohonan=p.nomor
WHERE ba.flaghapus=0 AND ba.`tanggalba` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.id AS `idpermohonan`,
'Kondisi Meter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM `__tmp_sambung_kembali` p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.id AS `idpermohonan`,
'Merk Meter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
m.id AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM `__tmp_sambung_kembali` p
JOIN `ba_sambungkembali` ba ON ba.nomorpermohonan=p.nomor
JOIN __tmp_merkmeter m ON m.merk=ba.`merkmeter`
WHERE ba.flaghapus=0 AND ba.`tanggalba` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.id AS `idpermohonan`,
'No Segel' AS `parameter`,
'string' AS `tipedata`,
ba.`nosegelmeter` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM `__tmp_sambung_kembali` p
JOIN `ba_sambungkembali` ba ON ba.nomorpermohonan=p.nomor
WHERE ba.flaghapus=0 AND ba.`tanggalba` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.id AS `idpermohonan`,
'Seri Meter' AS `parameter`,
'string' AS `tipedata`,
ba.`serimeter` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM `__tmp_sambung_kembali` p
JOIN `ba_sambungkembali` ba ON ba.nomorpermohonan=p.nomor
WHERE ba.flaghapus=0 AND ba.`tanggalba` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.id AS `idpermohonan`,
'Stan Meter' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
ba.`angkameter` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM `__tmp_sambung_kembali` p
JOIN `ba_sambungkembali` ba ON ba.nomorpermohonan=p.nomor
WHERE ba.flaghapus=0 AND ba.`tanggalba` IS NOT NULL