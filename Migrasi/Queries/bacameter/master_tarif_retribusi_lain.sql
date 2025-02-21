SELECT
 @idpdam,
 id AS idretribusilain,
 kode AS koderetribusilain,
 keterangan AS namaretribusilain,
 retribusilain AS retribusi,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 byretribusi_lain;