DROP TABLE IF EXISTS __tmp_tutup_total;
CREATE TABLE __tmp_tutup_total AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_pemutusan_sementara`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

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
FROM __tmp_tutup_total p