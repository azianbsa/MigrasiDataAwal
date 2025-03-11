DROP TABLE IF EXISTS __tmp_baliknama;
CREATE TABLE __tmp_baliknama AS
SELECT
@id:=@id+1 AS id,
nomor
FROM `permohonan_balik_nama`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Alamat Pemilik Baru' AS `parameter`,
'string' AS `tipedata`,
b.`alamat` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_baliknama p
JOIN `permohonan_balik_nama` b ON b.`nomor`=p.nomor
UNION ALL
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
FROM __tmp_baliknama p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Email Baru' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_baliknama p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Kepemilikan Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
1 AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_baliknama p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Nama Baru' AS `parameter`,
'string' AS `tipedata`,
b.`baru` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_baliknama p
JOIN `permohonan_balik_nama` b ON b.`nomor`=p.nomor
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Nama Lama' AS `parameter`,
'string' AS `tipedata`,
b.`lama` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_baliknama p
JOIN `permohonan_balik_nama` b ON b.`nomor`=p.nomor
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Nama Pemilik Baru' AS `parameter`,
'string' AS `tipedata`,
b.`baru` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_baliknama p
JOIN `permohonan_balik_nama` b ON b.`nomor`=p.nomor
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'No HP Baru' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_baliknama p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'No KK Baru' AS `parameter`,
'string' AS `tipedata`,
b.`nokk_baru` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_baliknama p
JOIN `permohonan_balik_nama` b ON b.`nomor`=p.nomor
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'No KTP Baru' AS `parameter`,
'string' AS `tipedata`,
b.`noktp_baru` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_baliknama p
JOIN `permohonan_balik_nama` b ON b.`nomor`=p.nomor
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'No Telp Baru' AS `parameter`,
'string' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_baliknama p
UNION ALL
SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Pekerjaan Baru' AS `parameter`,
'int' AS `tipedata`,
NULL AS `valuestring`,
NULL AS `valuedecimal`,
-1 AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_baliknama p