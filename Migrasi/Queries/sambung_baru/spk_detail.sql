DROP TABLE IF EXISTS __tmp_pendaftaran;
CREATE TABLE __tmp_pendaftaran AS
SELECT
@id:=@id+1 AS id,
nomorreg
FROM `pendaftaran`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

DROP TABLE IF EXISTS __tmp_diameter;
CREATE TABLE __tmp_diameter AS
SELECT
@id:=@id+1 AS id,
kodediameter,
aktif
FROM
diameter,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
aktif
FROM
golongan,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_peruntukan;
CREATE TEMPORARY TABLE __tmp_peruntukan AS
SELECT
@id:=@id+1 AS id,
peruntukan
FROM
`peruntukan`,
(SELECT @id:=0) AS id;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Diameter' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
d.`id` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorreg`
LEFT JOIN __tmp_diameter d ON d.`kodediameter`=pp.`pipa_instalasi` AND d.`aktif`=1
WHERE pp.`tglspko` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Diameter Distribusi' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
d.`id` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorreg`
LEFT JOIN __tmp_diameter d ON d.`kodediameter`=pp.`pipa_instalasi` AND d.`aktif`=1
WHERE pp.`tglspko` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Golongan' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
g.`id` AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorreg`
LEFT JOIN `__tmp_golongan` g ON g.`kodegol`=pp.`kodegol` AND g.`aktif`=1
WHERE pp.`tglspko` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Jarak Pipa Distribusi (Meter)' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
pp.`jarak_pipa_dis_ke_meter` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorreg`
WHERE pp.`tglspko` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Keterangan Survey' AS `parameter`,
'string' AS `tipedata`,
pp.`keteranganopname` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorreg`
WHERE pp.`tglspko` IS NOT NULL
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Peruntukan' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
r.id AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `pendaftaran` pp ON pp.`nomorreg`=p.`nomorreg`
LEFT JOIN __tmp_peruntukan r ON r.peruntukan=pp.peruntukan
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Tanggal Realisasi Survey' AS `parameter`,
'date' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
pp.tglspko AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
JOIN `spk_opname_sambung_baru` pp ON pp.`nomorreg`=p.`nomorreg`
WHERE pp.`tglspko` IS NOT NULL