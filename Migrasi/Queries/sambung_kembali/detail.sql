DROP TEMPORARY TABLE IF EXISTS __tmp_sambung_kembali;
CREATE TEMPORARY TABLE __tmp_sambung_kembali AS
SELECT
@id := @id+1 AS ID,
p.nomor
FROM permohonan_sambung_kembali P
,(SELECT @id := @lastid) AS id
WHERE p.flaghapus=0;

SELECT
@idpdam AS `idpdam`,
p.id AS `idpermohonan`,
'Ditagih Setelah' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM `__tmp_sambung_kembali` p