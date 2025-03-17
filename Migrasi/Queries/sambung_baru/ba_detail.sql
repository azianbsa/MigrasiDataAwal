DROP TABLE IF EXISTS __tmp_pendaftaran;
CREATE TABLE __tmp_pendaftaran AS
SELECT
@id:=@id+1 AS id,
nomorreg
FROM `pendaftaran`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

DROP TABLE IF EXISTS __tmp_kondisimeter;
CREATE TABLE __tmp_kondisimeter AS
SELECT
@id:=@id+1 AS id,
kodekondisi,
kondisi
FROM
kondisimeter
,(SELECT @id:=0) AS id;

DROP TABLE IF EXISTS __tmp_merkmeter;
CREATE TABLE __tmp_merkmeter AS
SELECT
@id:=@id+1 AS id,
merk
FROM
merkmeter,
(SELECT @id:=0) AS id;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Keterangan' AS `parameter`,
'string' AS `tipedata`,
b.`keteranganmeter` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `rab` b ON b.`nomorreg`=p.`nomorreg`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tanggalba` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Kondisi Meter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
k.`id` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `rab` b ON b.`nomorreg`=p.`nomorreg`
LEFT JOIN `__tmp_kondisimeter` k ON k.`kodekondisi`=b.`kondisimeter`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tanggalba` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Merek Meter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
m.`id` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `rab` b ON b.`nomorreg`=p.`nomorreg`
LEFT JOIN `__tmp_merkmeter` m ON m.`merk`=b.`merkmeter`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tanggalba` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'No Segel' AS `parameter`,
'string' AS `tipedata`,
b.`nosegelmeter` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `rab` b ON b.`nomorreg`=p.`nomorreg`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tanggalba` IS NOT NULL
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
FROM __tmp_pendaftaran p
JOIN `rab` b ON b.`nomorreg`=p.`nomorreg`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tanggalba` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Stan Meter' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
b.`stanawalpasang` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `rab` b ON b.`nomorreg`=p.`nomorreg`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tanggalba` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Tanggal Pasang' AS `parameter`,
'date' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
b.`tglpasang` AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `rab` b ON b.`nomorreg`=p.`nomorreg`
WHERE b.`flaghapus`=0 AND b.`nomorba` IS NOT NULL AND b.`tanggalba` IS NOT NULL