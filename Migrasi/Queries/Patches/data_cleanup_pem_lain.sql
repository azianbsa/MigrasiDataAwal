UPDATE [table] SET kodepemeliharaanlain='-' WHERE kodepemeliharaanlain='';

REPLACE INTO bypemeliharaan_lain (
 kode,
 keterangan,
 pemeliharaanlain
)
SELECT
kodepemeliharaanlain,
'-',
0
FROM [table]
WHERE kodepemeliharaanlain NOT IN (SELECT kode FROM bypemeliharaan_lain)
GROUP BY kodepemeliharaanlain;