UPDATE [table] SET kodekolektif='-' WHERE kodekolektif='';

REPLACE INTO kolektif (
 kodekolektif,
 kolektif,
 ket,
 alamat
)
SELECT
kodekolektif,
kodekolektif,
NULL,
NULL
FROM [table]
WHERE kodekolektif NOT IN (SELECT kodekolektif FROM kolektif)
GROUP BY kodekolektif;