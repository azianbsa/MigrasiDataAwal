﻿SELECT
@idpdam AS idpdam,
idkelainan AS idkelainan,
kodekelainan AS kodekelainan,
kelainan AS kelainan,
'Biasa' AS jeniskelainan,
deskripsi AS deskripsi,
idx AS posisi,
0 AS blokir,
aktif AS status,
0 AS taksirotomatis,
0 AS requestbacaulangotomatis,
0 AS flaghapus,
NOW() AS waktuupdate
FROM [bacameter].kelainan;