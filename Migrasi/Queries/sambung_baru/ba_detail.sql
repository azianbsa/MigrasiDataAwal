SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';

SELECT
@idpdam AS `idpdam`,
b.`idpermohonan` AS `idpermohonan`,
c.`parameter` AS `parameter`,
c.`tipedata` AS `tipedata`,
CASE
	WHEN c.parameter='Keterangan' THEN ''
	WHEN c.parameter='No Segel' THEN ''
	WHEN c.parameter='Seri Meter' THEN ''
END AS `valuestring`,
CASE
	WHEN c.parameter='Stan Meter' THEN 0
END AS `valuedecimal`,
CASE
	WHEN c.parameter='Kondisi Meter' THEN 1
	WHEN c.parameter='Merk Meter' THEN -1
END AS `valueinteger`,
CASE
	WHEN c.parameter='Tanggal Pasang' THEN a.tgl_bst
END AS `valuedate`,
NULL AS `valuebool`,
a.`tgl_bst` AS `waktuupdate`
FROM `t_pelanggan_reg` a
JOIN `sambunganbaru` b ON b.`no_reg`=a.`no_reg`
JOIN `maros_awal`.`tipepermohonandetailba` c ON c.`idtipepermohonan`=b.idtipepermohonan
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
AND a.`no_bst`<>'-'
AND a.no_reg IN (
'0005/PMP/HL/VI/2025'
)