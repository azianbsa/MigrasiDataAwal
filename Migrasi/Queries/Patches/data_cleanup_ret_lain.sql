UPDATE [table] SET koderetribusilain='-' WHERE koderetribusilain='';

REPLACE INTO byretribusi_lain (kode, keterangan, retribusilain)
SELECT
koderetribusilain,
'-',
0
FROM [table]
WHERE koderetribusilain NOT IN (SELECT kode FROM byretribusi_lain)
GROUP BY koderetribusilain;