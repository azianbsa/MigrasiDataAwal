SET @tgladu_awal='2014-01-04';
SET @tgladu_akhir='2025-07-02';

SELECT
@idpdam AS `idpdam`,
a.`idpermohonan` AS `idpermohonan`,
b.`parameter` AS `parameter`,
b.`tipedata` AS `tipedata`,
CASE 
	WHEN b.`parameter`='Alamat Pelapor' THEN TRIM(a.`alamat`)
	WHEN b.`parameter`='HP Pelapor' THEN a.telp
	WHEN b.`parameter`='Nama Pelapor' THEN TRIM(a.nama)
	WHEN b.`parameter`='Telp. Pelapor' THEN a.telp
END AS `valuestring`,
NULL AS `valuedecimal`,
CASE WHEN b.`parameter`='Bagian' THEN COALESCE(u.idurusan,-1) END AS `valueinteger`,
NULL AS `valuedate`,
NULL AS `valuebool`,
a.tgladu AS `waktuupdate`
FROM `pengaduannonplg` a
JOIN `maros_awal`.`tipepermohonan` t ON t.`kodejenisnonair`=a.`jns`
JOIN `maros_awal`.`tipepermohonandetail` b ON b.`idtipepermohonan`=t.`idtipepermohonan`
LEFT JOIN `maros_awal`.`urusanmaros` u ON u.kodeurusan=a.id_subagian
WHERE a.idpermohonan=5114
-- a.`tgladu`>=@tgladu_awal
-- AND a.`tgladu`<@tgladu_akhir