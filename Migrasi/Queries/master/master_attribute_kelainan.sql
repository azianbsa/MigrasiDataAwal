SELECT
@idpdam,
idkelainan AS idkelainan,
kodekelainan AS kodekelainan,
kelainan AS kelainan,
'Biasa' AS jeniskelainan,
deskripsi,
idx AS posisi,
0 AS blokir,
aktif AS STATUS,
0 AS taksirotomatis,
0 AS requestbacaulangotomatis,
0 AS flaghapus,
NOW() AS waktuupdate
FROM `kotaparepare_bacameter`.kelainan;