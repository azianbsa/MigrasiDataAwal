SET @tgl_reg_awal='2014-11-21';
SET @tgl_reg_akhir='2025-06-20';

SELECT
@idpdam AS idpdam,
a.`idpermohonan` AS idpermohonan,
9 AS idjenisnonair,
n.`idnonair` AS idnonair,
a.`no_rab` AS nomorrab,
a.`tgl_rab` AS tanggalrab,
a.`no_bppi` AS nomorbppi,
a.`tgl_bppi` AS tanggalbppi,
-1 AS iduserbppi,
-1 AS iduser,
a.`tgl_rab` AS tanggalkadaluarsa,
'PAKET RAB SAM BARU 1/2' AS persilnamapaket,
0 AS persilflagdialihkankevendor,
0 AS persilflagbiayadibebankankepdam,
c.jumlah AS persilsubtotal,
0 AS persildibebankankepdam,
c.jumlah AS persiltotal,
NULL AS distribusinamapaket,
0 AS distribusiflagdialihkankevendor,
0 AS distribusiflagbiayadibebankankepdam,
0 AS distribusisubtotal,
0 AS distribusidibebankankepdam,
0 AS distribusitotal,
c.jumlah AS rekapsubtotal,
0 AS rekapdibebankankepdam,
c.jumlah AS rekaptotal,
0 AS flagrablainnya,
0 AS flagbatal,
NULL AS idalasanbatal,
a.`tgl_rab` AS waktuupdate
FROM `sambungkembali` a
JOIN (
SELECT SUM(jumlah) AS jumlah FROM `maros_awal`.`rabmaros` WHERE namapaket='PAKET RAB SAM BARU 1/2'
) c ON 1=1
JOIN `nonairmaros` n ON n.`nomornonair`=a.`no_reg` AND n.`idjenisnonair`=9 AND n.`keterangan`='rab'
WHERE a.`tgl_reg`>=@tgl_reg_awal
AND a.`tgl_reg`<@tgl_reg_akhir
AND a.`no_rab`<>'-'