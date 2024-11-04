UPDATE [table] SET kodegol='-' WHERE kodegol='';

REPLACE INTO golongan (
 kodegolyangberlaku,
 periodemulaiberlaku,
 kodegol,
 golongan,
 aktif
)
SELECT
CONCAT ('100001.', kodegol),
'100001',
kodegol,
kodegol,
1
FROM [table]
WHERE kodegol NOT IN (SELECT kodegol FROM golongan WHERE aktif=1)
GROUP BY kodegol;