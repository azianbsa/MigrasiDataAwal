SELECT
@idpdam AS idpdam,
b.`idnonair` AS idnonair,
a.`no_byr` AS nomortransaksi,
1 AS statustransaksi,
a.`tgl_byr` AS waktutransaksi,
YEAR(a.`tgl_byr`) AS tahuntransaksi,
um.`iduser` AS iduser,
l.`idloket` AS idloket,
NULL AS idkolektiftransaksi,
NULL AS idalasanbatal,
a.`ket` AS keterangan,
a.`tgl_byr` AS waktuupdate
FROM `t_jurair` a
JOIN `migrasi_nonair` b ON b.`no_reg`=a.`no_reg` AND b.`no_byr`=a.`no_byr` AND b.`jns`=a.`jns`
LEFT JOIN `t_user` u ON u.`NO_ID`=a.`opr`
LEFT JOIN `usermaros` um ON um.`nama`=u.`NAMA_USER`
LEFT JOIN `loketmaros` l ON l.`kodeloket`=a.`loket`
WHERE a.`no_byr`<>'-' 
AND a.`tgl_byr`>='2023-01-01'
AND a.`tgl_byr`<'2025-06-01'