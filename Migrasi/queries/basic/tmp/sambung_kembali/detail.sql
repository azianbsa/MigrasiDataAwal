SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';

SELECT
@idpdam AS `idpdam`,
a.idpermohonan AS `idpermohonan`,
b.`parameter` AS `parameter`,
b.`tipedata` AS `tipedata`,
CASE
	WHEN b.parameter='Nama Pelapor' THEN a.nama
	WHEN b.parameter='Alamat Pelapor' THEN a.alamat
	WHEN b.parameter='Telp. Pelapor' THEN a.telp
	WHEN b.parameter='HP Pelapor' THEN a.telp
	WHEN b.parameter='Ditagih Setelah' THEN ''
END AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `sambungkembali` a
JOIN `maros_awal`.`tipepermohonandetail` b ON b.`idtipepermohonan`=a.idtipepermohonan
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir