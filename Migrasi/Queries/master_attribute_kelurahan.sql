SELECT
 @idpdam,
 k.id AS idkelurahan,
 k.kodekelurahan,
 k.kelurahan AS namakelurahan,
 kc.id AS idkecamatan,
 k.jumlahjiwa,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 kelurahan k
 JOIN kecamatan kc ON kc.kodekecamatan = k.kodekecamatan;