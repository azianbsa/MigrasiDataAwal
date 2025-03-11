DROP TABLE IF EXISTS __tmp_pengaduan;
CREATE TABLE __tmp_pengaduan AS
SELECT
@id:=@id+1 AS id,
p.nomor,
p.`kategori`
FROM pengaduan p
JOIN `__tmp_tipepermohonan` t ON t.`kodejenisnonair`=p.`kategori`
,(SELECT @id:=@lastid) AS id
WHERE `flaghapus`=0 AND `flagpel`=1;

SELECT
@idpdam AS `idpdam`,
p.`id` AS `idpermohonan`,
'Ditagih Setelah' AS `parameter`,
'string' AS `tipedata`,
b.`ditagih_setelah` AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
NOW() AS `waktuupdate`
FROM __tmp_pengaduan p
JOIN `pengaduan` b ON b.nomor=p.`nomor`
WHERE p.`kategori` IN (
'JNS-105'
)