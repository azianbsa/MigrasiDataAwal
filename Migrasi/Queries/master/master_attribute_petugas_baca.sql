SELECT
@idpdam,
kodepetugas AS idpetugasbaca,
kodepetugas AS kodepetugasbaca,
nama AS petugasbaca,
LOWER(REPLACE(nama,' ','_')) AS namauser,
'' AS PASSWORD,
'Pembaca Meter' AS jenispembaca,
'' AS alamat,
'1000-01-01' AS tgllahir,
'' AS nohp,
'1000-01-01' AS tglmulaikerja,
NULL AS fotopetugasbaca,
'' AS keterangan,
0 AS flagforeman,
1 AS STATUS,
0 AS flaghapus,
NOW() AS waktucreate,
NOW() AS waktuupdate
FROM
`petugasbaca`