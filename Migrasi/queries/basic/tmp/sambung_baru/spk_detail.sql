SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';

SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
c.`parameter` AS `parameter`,
c.`tipedata` AS `tipedata`,
CASE
	WHEN c.parameter='Keterangan Survey' THEN ''
END AS `valuestring`,
CASE
	WHEN c.parameter='Jarak Pipa Distribusi (Meter)' THEN 0
END AS `valuedecimal`,
CASE
	WHEN c.parameter='Diameter' THEN d.iddiameter
	WHEN c.parameter='Diameter Distribusi' THEN d.iddiameter
	WHEN c.parameter='Golongan' THEN COALESCE(g.`idgolongan`,30)
	WHEN c.parameter='Peruntukan' THEN COALESCE(bg.`idperuntukan`,1)
END AS `valueinteger`,
CASE
	WHEN c.parameter='Tanggal Realisasi Survey' THEN a.tgl_spko
END AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_spko` AS waktuupdate
FROM `t_pelanggan_reg` a
JOIN `sambunganbaru` b ON b.`no_reg`=a.`no_reg`
JOIN `maros_awal`.`tipepermohonandetailspk` c ON c.`idtipepermohonan`=b.idtipepermohonan
LEFT JOIN `maros_awal`.`diametermaros` d ON d.`kodediameter`=a.`dia_met`
LEFT JOIN `maros_awal`.`golonganmaros` g ON g.kodegolongan=a.`KDGT`
LEFT JOIN `maros_awal`.`t_fg_bgn` bg ON bg.`kd_fg_bgn`=a.`fgbgn`
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
AND a.`no_spko`<>'-'
AND a.no_reg IN (
'0005/PMP/HL/VI/2025'
)