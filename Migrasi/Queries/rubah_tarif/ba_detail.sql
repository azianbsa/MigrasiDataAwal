DROP TABLE IF EXISTS __tmp_rubah_tarif;
CREATE TABLE __tmp_rubah_tarif AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_rubah_gol`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Keterangan' AS `parameter`,
'string' AS `tipedata`,
b.keterangan_ba AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_rubah_tarif p
JOIN `permohonan_rubah_gol` b ON b.`nomor`=p.nomor
WHERE b.`flag_ba_pengecekan`=1