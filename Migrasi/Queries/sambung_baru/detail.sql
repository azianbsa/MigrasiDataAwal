DROP TABLE IF EXISTS __tmp_pendaftaran;
CREATE TABLE __tmp_pendaftaran AS
SELECT
@id:=@id+1 AS id,
nomorreg
FROM `pendaftaran`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Denah Lokasi' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
UNION ALL
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
FROM __tmp_pendaftaran p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Foto Copy IMB/Keterangan Lurah' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Foto Copy KK' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
1 AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Foto Copy KTP' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
1 AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Map Snelhecter Plastik' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Meterai 10.000' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Surat Pernyataan' AS `parameter`,
'bool' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
0 AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pendaftaran p;