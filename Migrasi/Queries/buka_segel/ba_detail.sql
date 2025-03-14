DROP TABLE IF EXISTS __tmp_buka_segel;
CREATE TABLE __tmp_buka_segel AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_bukasegel`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Alasan Putus' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_buka_segel p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Keterangan' AS `parameter`,
'string' AS `tipedata`,
ba.memo AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_buka_segel p
JOIN `ba_bukasegel` ba ON ba.nomorpermohonan=p.`nomor`