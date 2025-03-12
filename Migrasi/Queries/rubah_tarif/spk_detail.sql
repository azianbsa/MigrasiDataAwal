DROP TABLE IF EXISTS __tmp_rubah_tarif;
CREATE TABLE __tmp_rubah_tarif AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_rubah_gol`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

DROP TEMPORARY TABLE IF EXISTS __tmp_diameter;
CREATE TEMPORARY TABLE __tmp_diameter AS
SELECT
@id:=@id+1 AS id,
kodediameter,
ukuran
FROM
diameter,
(SELECT @id:=0) AS id;

DROP TEMPORARY TABLE IF EXISTS __tmp_golongan;
CREATE TEMPORARY TABLE __tmp_golongan AS
SELECT
@id:=@id+1 AS id,
kodegol,
golongan
FROM
golongan,
(SELECT @id:=0) AS id;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Administrasi Lain Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_rubah_tarif p
JOIN `permohonan_rubah_gol` b ON b.`nomor`=p.nomor
WHERE b.`flag_spk_pengecekan`=1
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
FROM __tmp_rubah_tarif p
JOIN `permohonan_rubah_gol` b ON b.`nomor`=p.nomor
LEFT JOIN __tmp_diameter d ON d.kodediameter=b.kodediameter_baru
WHERE b.`flag_spk_pengecekan`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Golongan Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
g.id AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_rubah_tarif p
JOIN `permohonan_rubah_gol` b ON b.`nomor`=p.nomor
LEFT JOIN __tmp_golongan g ON g.kodegol=b.baru
WHERE b.`flag_spk_pengecekan`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Nomor Regis' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_rubah_tarif p
JOIN `permohonan_rubah_gol` b ON b.`nomor`=p.nomor
WHERE b.`flag_spk_pengecekan`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Pemeliharaan Lain Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_rubah_tarif p
JOIN `permohonan_rubah_gol` b ON b.`nomor`=p.nomor
WHERE b.`flag_spk_pengecekan`=1
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Retribusi Lain Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_rubah_tarif p
JOIN `permohonan_rubah_gol` b ON b.`nomor`=p.nomor
WHERE b.`flag_spk_pengecekan`=1