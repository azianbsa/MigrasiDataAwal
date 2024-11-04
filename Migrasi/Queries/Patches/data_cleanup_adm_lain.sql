UPDATE [table] SET kodeadministrasilain='-' WHERE kodeadministrasilain='';

REPLACE INTO byadministrasi_lain (
 kode,
 keterangan,
 administrasilain
)
SELECT
kodeadministrasilain,
'-',
0
FROM [table]
WHERE kodeadministrasilain NOT IN (SELECT kode FROM byadministrasi_lain)
GROUP BY kodeadministrasilain;