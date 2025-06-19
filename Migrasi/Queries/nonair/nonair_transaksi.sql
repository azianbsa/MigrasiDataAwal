SELECT
@idpdam AS idpdam,
-1 AS idnonair,
`no_byr` AS nomortransaksi,
1 AS statustransaksi,
`tgl_byr` AS waktutransaksi,
YEAR(`tgl_byr`) AS tahuntransaksi,
-1 AS iduser,
l.idloket AS idloket,
NULL AS idkolektiftransaksi,
NULL AS idalasanbatal,
`ket` AS keterangan,
`tgl_byr` AS waktuupdate
FROM `t_jurair` j
LEFT JOIN `loketmaros` l ON l.kodeloket=j.`loket`