SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';

SELECT
@idpdam AS idpdam,
b.`idpermohonan` AS idpermohonan,
b.`no_bst` AS nomorba,
b.`tgl_bst` AS tanggalba,
-1 AS iduser,
NULL AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
0 AS flagbatal,
NULL AS idalasanbatal,
0 AS flag_dari_verifikasi,
'Berhasil Dikerjakan' AS statusberitaacara,
a.`tgl_bst` AS waktuupdate
FROM `t_pelanggan_reg` a
JOIN `sambunganbaru` b ON b.`no_reg`=a.`no_reg`
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
AND a.`no_bst`<>'-'
AND a.no_reg IN (
'0005/PMP/HL/VI/2025'
)