DROP TABLE IF EXISTS __tmp_rotasimeter;
CREATE TABLE __tmp_rotasimeter AS
SELECT
@id := @id+1 AS id,
p.nosamb,
p.periode
FROM `rotasimeter` p
,(SELECT @id := @lastid) AS id;

DROP TABLE IF EXISTS __tmp_merkmeter;
CREATE TABLE __tmp_merkmeter AS
SELECT
@id:=@id+1 AS id,
merk
FROM
merkmeter,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_diameter;
CREATE TEMPORARY TABLE __tmp_diameter AS
SELECT
@id:=@id+1 AS id,
kodediameter,
aktif
FROM
diameter,
(SELECT @id:=0) AS id;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Angka Meter Baru' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
b.`angkameter_baru` AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_rotasimeter p
JOIN `rotasimeter` b ON b.`nosamb`=p.`nosamb` AND b.`periode`=p.`periode` AND b.`flag_ba`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Diameter Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
d.id AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_rotasimeter p
JOIN `rotasimeter` b ON b.`nosamb`=p.`nosamb` AND b.`periode`=p.`periode` AND b.`flag_ba`=1
LEFT JOIN __tmp_diameter d ON d.kodediameter=b.`kodediameter_baru`
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Keterangan' AS `parameter`,
'string' AS `tipedata`,
b.`keterangan_ba` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_rotasimeter p
JOIN `rotasimeter` b ON b.`nosamb`=p.`nosamb` AND b.`periode`=p.`periode` AND b.`flag_ba`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Merk Meter Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
m.id AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_rotasimeter p
JOIN `rotasimeter` b ON b.`nosamb`=p.`nosamb` AND b.`periode`=p.`periode` AND b.`flag_ba`=1
LEFT JOIN __tmp_merkmeter m ON m.merk=b.`merkmeter_baru`
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
NOW() AS `waktuupdate`
FROM __tmp_rotasimeter p
JOIN `rotasimeter` b ON b.`nosamb`=p.`nosamb` AND b.`periode`=p.`periode` AND b.`flag_ba`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Serimeter Baru' AS `parameter`,
'string' AS `tipedata`,
b.`serimeter_baru` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_rotasimeter p
JOIN `rotasimeter` b ON b.`nosamb`=p.`nosamb` AND b.`periode`=p.`periode` AND b.`flag_ba`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Stan Angkat' AS `parameter`,
'decimal' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_rotasimeter p
JOIN `rotasimeter` b ON b.`nosamb`=p.`nosamb` AND b.`periode`=p.`periode` AND b.`flag_ba`=1
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
FROM __tmp_rotasimeter p
JOIN `rotasimeter` b ON b.`nosamb`=p.`nosamb` AND b.`periode`=p.`periode` AND b.`flag_ba`=1