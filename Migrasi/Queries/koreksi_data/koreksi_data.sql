SET @maxid=(SELECT COALESCE(MAX(idkoreksi),0) AS maxid FROM `kotaparepare_dataawal`.`tampung_koreksi_data` WHERE idpdam=@idpdam);

SELECT
@idpdam AS `idpdam`,
@id:=@id+1 AS `idkoreksi`,
0 AS idpermohonan,
'Manual' AS `sumberperubahan`,
DATE(p.`tanggal_koreksi`) AS `waktukoreksi`,
TIME(p.`tanggal_koreksi`) AS `jamkoreksi`,
u.`iduser` AS `iduser`,
pe.`idpelangganair` AS `idpelangganair`,
IF(`tanggal_verifikasi` IS NOT NULL AND `flagprosesdata`=1,1,0) AS `flagverifikasi`,
p.`tanggal_verifikasi` AS `waktuverifikasi`,
p.`tanggal_koreksi` AS `waktuupdate`,
p.`nomor` AS nomor
FROM `permohonan_koreksi_data` p
JOIN `kotaparepare_dataawal`.`tampung_master_pelanggan_air` pe ON pe.`nosamb`=p.`nosamb`
LEFT JOIN `kotaparepare_dataawal`.`master_user` u ON u.nama=p.`user_koreksi`
,(SELECT @id:=@maxid) AS id
WHERE p.`flaghapus`=0
AND p.`nomor` NOT IN (SELECT nomor FROM `kotaparepare_dataawal`.`tampung_koreksi_data` WHERE idpdam=@idpdam)
AND DATE_FORMAT(p.`tanggal_koreksi`,'%Y%m') BETWEEN 202502 AND 202504