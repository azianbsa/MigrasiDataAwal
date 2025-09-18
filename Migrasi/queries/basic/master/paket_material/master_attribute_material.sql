SELECT
@idpdam,
@id := @id+1 AS idmaterial,
kodebarang AS kodematerial,
Deskripsi AS namamaterial,
0 AS materiallimbah,
Unit AS satuan,
hargabeli AS hargabeli,
hargajual AS hargajual,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
barang,
(SELECT @id := 0) AS id