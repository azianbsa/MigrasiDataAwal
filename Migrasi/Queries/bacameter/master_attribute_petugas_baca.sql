SELECT
@idpdam,
idpetugas AS idpetugasbaca,
kodepetugas AS kodepetugasbaca,
nama AS petugasbaca,
namauser,
`passworduser` AS PASSWORD,
'Pembaca Meter' AS jenispembaca,
alamat,
'1000-01-01' AS tgllahir,
NULL AS nohp,
'1000-01-01' AS tglmulaikerja,
NULL AS fotopetugasbaca,
'' AS keterangan,
aktif AS STATUS,
0 AS flaghapus,
NOW() AS waktucreate,
NOW() AS waktuupdate
FROM
`petugasbaca`