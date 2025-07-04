SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';

SELECT
@idpdam AS `idpdam`,
b.idpermohonan AS `idpermohonan`,
c.`parameter` AS `parameter`,
c.`tipedata` AS `tipedata`,
CASE
	WHEN c.parameter='Ditagih Setelah' THEN ''
END AS `valuestring`,
NULL AS `valuedecimal`,
NULL AS `valueinteger`,
NULL AS `valuedate`,
CASE
	WHEN c.parameter='Denah Lokasi' THEN 0
	WHEN c.parameter='Foto Copy IMB/Keterangan Lurah' THEN 0
	WHEN c.parameter='Foto Copy KK' THEN 1
	WHEN c.parameter='Foto Copy KTP' THEN 1
	WHEN c.parameter='Map Snelhecter Plastik' THEN 0
	WHEN c.parameter='Meterai 10.000' THEN 0
	WHEN c.parameter='Surat Pernyataan' THEN 0
END AS `valuebool`,
a.`tgl_reg` AS `waktuupdate`
FROM `t_pelanggan_reg` a
JOIN `sambunganbaru` b ON b.`no_reg`=a.`no_reg`
JOIN `maros_awal`.`tipepermohonandetail` c ON c.`idtipepermohonan`=b.idtipepermohonan
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir