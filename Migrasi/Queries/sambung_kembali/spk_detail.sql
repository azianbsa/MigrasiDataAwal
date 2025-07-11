SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';

SELECT
@idpdam AS `idpdam`,
a.`idpermohonan` AS `idpermohonan`,
b.`parameter` AS `parameter`,
b.`tipedata` AS `tipedata`,
CASE
	WHEN b.parameter='Keterangan Survey' THEN ''
END AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_spko` AS waktuupdate
FROM `sambungkembali` a
JOIN `maros_awal`.`tipepermohonandetailspk` b ON b.`idtipepermohonan`=a.idtipepermohonan
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
AND a.`no_spko`<>'-'