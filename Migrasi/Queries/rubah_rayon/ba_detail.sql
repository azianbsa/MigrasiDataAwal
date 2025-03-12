DROP TABLE IF EXISTS __tmp_rubahrayon;
CREATE TABLE __tmp_rubahrayon AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_rubah_rayon`
,(SELECT @id:=@lastid) AS id
WHERE flaghapus=0;

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
NOW() AS `waktuupdate`
FROM __tmp_rubahrayon p
JOIN `permohonan_rubah_rayon` pp ON pp.`nomor`=p.nomor
WHERE pp.`flag_ba`=1