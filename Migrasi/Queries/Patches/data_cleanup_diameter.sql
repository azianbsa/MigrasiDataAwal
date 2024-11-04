UPDATE [table] SET kodediameter='-' WHERE kodediameter='';

REPLACE INTO diameter (
 kodediameteryangberlaku,
 periodemulaiberlaku,
 kodediameter,
 ukuran,
 administrasi,
 pemeliharaan,
 pelayanan,
 retribusi,
 dendapakai0,
 airlimbah,
 nomorba,
 aktif
)
SELECT
CONCAT ('100001.', kodediameter),
'100001',
kodediameter,
kodediameter,
0,
0,
0,
0,
0,
0,
'-',
1
FROM [table]
WHERE kodediameter NOT IN (SELECT kodediameter FROM diameter WHERE aktif=1)
GROUP BY kodediameter;