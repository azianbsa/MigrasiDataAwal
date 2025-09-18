SELECT
 @idpdam,
 id AS idongkos,
 kodeongkos AS kodeongkos,
 namaongkos AS namaongkos,
 0 AS ongkoslimbah,
 satuan AS satuan,
 tipe AS kelompok,
 kategori AS postbiaya,
 NULL AS perhitungan,
 NULL AS idpaketmaterial,
 jumlah_persen AS persentase,
 harga AS biaya,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 ongkos